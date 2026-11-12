//! App-owned staging for generated files. No JavaScript path is accepted for writing or cleanup.

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tauri::Manager;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const MAX_ENCODED_BYTES: usize = MAX_BYTES.div_ceil(3) * 4;
const MAX_STAGED_FILES: usize = 16;
const STAGED_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StagedGeneratedFile {
    pub token: String,
    pub path: String,
    pub size_bytes: u64,
}

struct OwnedFile {
    path: PathBuf,
    // Keep the inode alive until cleanup, so replacing the path cannot reuse its identity.
    file: File,
    created: Instant,
    _permit: OwnedSemaphorePermit,
}

impl OwnedFile {
    fn remove(&self) -> io::Result<bool> {
        let current = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if !current.is_file() || current.file_type().is_symlink() {
            return Err(io::Error::other("staged file identity changed"));
        }
        let owned = self.file.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if current.dev() != owned.dev() || current.ino() != owned.ino() || current.nlink() != 1
            {
                return Err(io::Error::other("staged file identity changed"));
            }
        }
        #[cfg(not(unix))]
        if current.created()? != owned.created()? || current.len() != owned.len() {
            return Err(io::Error::other("staged file identity changed"));
        }
        fs::remove_file(&self.path)?;
        Ok(true)
    }
}

impl Drop for OwnedFile {
    fn drop(&mut self) {
        // The path was created here with create_new and is checked against the still-open file.
        let _ = self.remove();
    }
}

struct GeneratedFiles {
    root: PathBuf,
    files: Mutex<HashMap<String, OwnedFile>>,
    permits: Arc<Semaphore>,
}

impl GeneratedFiles {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            files: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(MAX_STAGED_FILES)),
        }
    }

    fn prune_expired(&self) -> io::Result<()> {
        let expired = {
            let mut files = self
                .files
                .lock()
                .map_err(|_| io::Error::other("staging unavailable"))?;
            let tokens: Vec<_> = files
                .iter()
                .filter(|(_, file)| file.created.elapsed() >= STAGED_TTL)
                .map(|(token, _)| token.clone())
                .collect();
            tokens
                .into_iter()
                .filter_map(|token| files.remove(&token))
                .collect::<Vec<_>>()
        };
        drop(expired);
        Ok(())
    }

    fn stage(&self, encoded: &str) -> io::Result<StagedGeneratedFile> {
        if encoded.is_empty() || encoded.len() > MAX_ENCODED_BYTES {
            return Err(io::Error::other("generated file is empty or too large"));
        }
        self.prune_expired()?;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| io::Error::other("too many staged generated files"))?;
        let bytes = STANDARD
            .decode(encoded)
            .map_err(|_| io::Error::other("invalid generated file encoding"))?;
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(io::Error::other("generated file is empty or too large"));
        }
        fs::create_dir_all(&self.root)?;
        let root_metadata = fs::symlink_metadata(&self.root)?;
        if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
            return Err(io::Error::other("invalid generated file directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700))?;
        }
        let root = self.root.canonicalize()?;
        let token = uuid::Uuid::new_v4().to_string();
        let path = root.join(format!("generated-{token}.bin"));
        let mut options = OpenOptions::new();
        options.write(true).read(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path)?;
        let mut owned = OwnedFile {
            path,
            file,
            created: Instant::now(),
            _permit: permit,
        };
        owned.file.write_all(&bytes)?;
        owned.file.sync_all()?;
        let path = owned
            .path
            .to_str()
            .ok_or_else(|| io::Error::other("generated file path is not UTF-8"))?
            .to_owned();
        let response = StagedGeneratedFile {
            token: token.clone(),
            path,
            size_bytes: bytes.len() as u64,
        };
        self.files
            .lock()
            .map_err(|_| io::Error::other("staging unavailable"))?
            .insert(token, owned);
        Ok(response)
    }

    fn cleanup(&self, token: &str) -> io::Result<bool> {
        let parsed = uuid::Uuid::parse_str(token)
            .map_err(|_| io::Error::other("invalid generated file token"))?;
        if parsed.to_string() != token {
            return Err(io::Error::other("invalid generated file token"));
        }
        let owned = self
            .files
            .lock()
            .map_err(|_| io::Error::other("staging unavailable"))?
            .remove(token);
        match owned {
            Some(file) => file.remove(),
            None => Ok(false),
        }
    }
}

struct AppGeneratedFiles(Arc<GeneratedFiles>);

fn host_error(operation: &str, reason: impl ToString) -> crate::commands::CommandError {
    flare_im_core_sdk_bindings_runtime::BindingBoundaryError::new(
        "hostFailed",
        operation,
        reason.to_string(),
    )
    .to_json()
}

fn store(app: &tauri::AppHandle) -> Result<Arc<GeneratedFiles>, crate::commands::CommandError> {
    if app.try_state::<AppGeneratedFiles>().is_none() {
        let root = app
            .path()
            .app_cache_dir()
            .map_err(|_| {
                host_error(
                    "host.stage_generated_file",
                    "generated file directory unavailable",
                )
            })?
            .join("generated-files");
        // manage does not replace a concurrently installed state.
        let _ = app.manage(AppGeneratedFiles(Arc::new(GeneratedFiles::new(root))));
    }
    Ok(app.state::<AppGeneratedFiles>().inner().0.clone())
}

/// Stage bounded generated bytes. Call cleanup with the returned token in a finally block.
#[tauri::command]
pub async fn sdk_stage_generated_file(
    app: tauri::AppHandle,
    bytes_base64: String,
) -> Result<StagedGeneratedFile, crate::commands::CommandError> {
    let store = store(&app)?;
    tokio::task::spawn_blocking(move || store.stage(&bytes_base64))
        .await
        .map_err(|_| {
            host_error(
                "host.stage_generated_file",
                "generated file staging interrupted",
            )
        })?
        .map_err(|error| host_error("host.stage_generated_file", error))
}

/// Removes only a file registered under this App's opaque token; no path is accepted.
#[tauri::command]
pub async fn sdk_cleanup_generated_file(
    app: tauri::AppHandle,
    token: String,
) -> Result<bool, crate::commands::CommandError> {
    let store = store(&app)?;
    tokio::task::spawn_blocking(move || store.cleanup(&token))
        .await
        .map_err(|_| {
            host_error(
                "host.cleanup_generated_file",
                "generated file cleanup interrupted",
            )
        })?
        .map_err(|error| host_error("host.cleanup_generated_file", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fixture() -> (PathBuf, GeneratedFiles) {
        let root =
            std::env::temp_dir().join(format!("flare-generated-test-{}", uuid::Uuid::new_v4()));
        (root.clone(), GeneratedFiles::new(root))
    }

    #[test]
    fn staged_bytes_are_exact_and_cleanup_is_token_scoped_and_idempotent() {
        let (root, store) = fixture();
        let bytes = b"\x89PNG\r\n\x1a\ngenerated";
        let staged = store.stage(&STANDARD.encode(bytes)).unwrap();
        assert_eq!(fs::read(&staged.path).unwrap(), bytes);
        assert_eq!(staged.size_bytes, bytes.len() as u64);
        let sibling = root.join("keep.txt");
        fs::write(&sibling, b"keep").unwrap();
        assert!(store.cleanup("../keep.txt").is_err());
        assert!(store.cleanup(&staged.token).unwrap());
        assert!(!store.cleanup(&staged.token).unwrap());
        assert_eq!(fs::read(&sibling).unwrap(), b"keep");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_or_oversized_data_does_not_create_a_file() {
        let (root, store) = fixture();
        assert!(store.stage("").is_err());
        assert!(store.stage("not base64").is_err());
        assert!(store.stage(&"A".repeat(MAX_ENCODED_BYTES + 1)).is_err());
        assert!(!root.exists());
    }

    #[test]
    fn app_state_drop_removes_its_staged_files() {
        let (root, store) = fixture();
        let staged = store.stage(&STANDARD.encode(b"generated")).unwrap();
        drop(store);
        assert!(!Path::new(&staged.path).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_replaced_path_is_not_deleted_by_token_cleanup() {
        let (root, store) = fixture();
        let staged = store.stage(&STANDARD.encode(b"generated")).unwrap();
        fs::remove_file(&staged.path).unwrap();
        fs::write(&staged.path, b"replaced!").unwrap();
        assert_eq!(
            store.cleanup(&staged.token).unwrap_err().to_string(),
            "staged file identity changed"
        );
        assert_eq!(fs::read(&staged.path).unwrap(), b"replaced!");
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_replacement_and_its_target_are_preserved() {
        use std::os::unix::fs::symlink;
        let (root, store) = fixture();
        let staged = store.stage(&STANDARD.encode(b"generated")).unwrap();
        let sibling = root.join("keep.txt");
        fs::write(&sibling, b"keep").unwrap();
        fs::remove_file(&staged.path).unwrap();
        symlink(&sibling, &staged.path).unwrap();
        assert_eq!(
            store.cleanup(&staged.token).unwrap_err().to_string(),
            "staged file identity changed"
        );
        assert!(
            fs::symlink_metadata(&staged.path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&sibling).unwrap(), b"keep");
        fs::remove_file(&staged.path).unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
