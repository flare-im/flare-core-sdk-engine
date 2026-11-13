//! Revoke-only write-ahead journal. No bearer, OS token, message or remote URL.
#[cfg(not(target_arch = "wasm32"))]
use super::hex_id;
use super::{ErrorCode, Result, digest, failure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub revocation_token: String,
    pub endpoint: String,
    pub provider: String,
}
pub(super) struct Journal {
    root: PathBuf,
    pub entries: Vec<Intent>,
    #[cfg(not(target_arch = "wasm32"))]
    _lease: std::fs::File,
}
impl Journal {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn open(root: PathBuf) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use std::{
                fs::{OpenOptions, symlink_metadata},
                io::Read,
            };
            let mut dirs = std::fs::DirBuilder::new();
            dirs.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                dirs.mode(0o700);
            }
            dirs.create(&root)
                .map_err(|_| failure(ErrorCode::DatabaseError))?;
            if symlink_metadata(&root)
                .map_err(|_| failure(ErrorCode::DatabaseError))?
                .file_type()
                .is_symlink()
            {
                return Err(failure(ErrorCode::DatabaseError));
            }
            let open = |path: &Path| -> Result<std::fs::File> {
                let mut options = OpenOptions::new();
                options.read(true).write(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let before = match symlink_metadata(path) {
                    Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => Some(meta),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    _ => return Err(failure(ErrorCode::DatabaseError)),
                };
                options.create_new(before.is_none());
                let file = options
                    .open(path)
                    .map_err(|_| failure(ErrorCode::DatabaseError))?;
                let meta = file
                    .metadata()
                    .map_err(|_| failure(ErrorCode::DatabaseError))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    let after =
                        symlink_metadata(path).map_err(|_| failure(ErrorCode::DatabaseError))?;
                    if meta.mode() & 0o077 != 0
                        || meta.nlink() != 1
                        || after.file_type().is_symlink()
                        || after.dev() != meta.dev()
                        || after.ino() != meta.ino()
                        || before
                            .as_ref()
                            .is_some_and(|v| v.dev() != meta.dev() || v.ino() != meta.ino())
                    {
                        return Err(failure(ErrorCode::DatabaseError));
                    }
                }
                Ok(file)
            };
            let lease = open(&root.join("push-revocations.lock"))?;
            lease
                .try_lock()
                .map_err(|_| failure(ErrorCode::DatabaseError))?;
            let file = open(&root.join("push-revocations.json"))?;
            let mut data = Vec::new();
            file.take(65537)
                .read_to_end(&mut data)
                .map_err(|_| failure(ErrorCode::DatabaseError))?;
            if data.len() > 65536 {
                return Err(failure(ErrorCode::DatabaseError));
            }
            let entries: Vec<Intent> = if data.is_empty() {
                vec![]
            } else {
                crate::shared::util::json::from_slice(&data)
                    .map_err(|_| failure(ErrorCode::DatabaseError))?
            };
            if entries.len() > 64
                || entries.iter().any(|e| {
                    !hex_id(&e.revocation_token)
                        || !hex_id(&e.endpoint)
                        || !super::valid_provider(&e.provider)
                })
            {
                return Err(failure(ErrorCode::DatabaseError));
            }
            Ok(Self {
                root,
                entries,
                _lease: lease,
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            Ok(Self {
                root,
                entries: vec![],
            })
        }
    }
    pub fn add(&mut self, intent: Intent) -> Result<()> {
        if self.entries.len() >= 64 {
            return Err(failure(ErrorCode::DatabaseError));
        }
        let mut next = self.entries.clone();
        next.push(intent);
        self.save(&next)?;
        self.entries = next;
        Ok(())
    }
    pub fn remove(&mut self, id: &str) -> Result<()> {
        let next = self
            .entries
            .iter()
            .filter(|v| digest(&v.revocation_token) != id)
            .cloned()
            .collect::<Vec<_>>();
        self.save(&next)?;
        self.entries = next;
        Ok(())
    }
    fn save(&self, entries: &[Intent]) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use std::{fs::OpenOptions, io::Write};
            let path = self
                .root
                .join(format!(".push-revocations-{}", uuid::Uuid::new_v4()));
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let result = (|| {
                let mut file = options.open(&path)?;
                file.write_all(&serde_json::to_vec(entries).map_err(std::io::Error::other)?)?;
                file.sync_all()?;
                std::fs::rename(&path, self.root.join("push-revocations.json"))?;
                std::fs::File::open(&self.root)?.sync_all()
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(&path);
                return Err(failure(ErrorCode::DatabaseError));
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = entries;
        }
        Ok(())
    }
}
