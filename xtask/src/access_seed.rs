//! `cargo xtask access-seed`：构建期种子。
//!
//! 各端构建脚本在打包前跑一次：从镜像拉最新的已签名接入清单写入种子文件（`access.seed` 配置就是这个
//! 文件的内容），并用要编进 App 的信任根校验。校验不过、已过期、没有引导源的清单不能进安装包：
//! 一份坏种子要等用户升级才换得掉。

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use flare_im_core_sdk::client::config::SdkConfig;
use flare_im_core_sdk::link::PlatformHttpFetch;
use flare_im_core_sdk::link::flare_access::manifest::MAX_DOCUMENT_BYTES;
use flare_im_core_sdk::link::flare_access::trust::{
    TrustRootConfig, TrustRoots, TrustStore, VerifiedAccessManifest,
};
use flare_im_core_sdk::link::flare_access::{FetchRequest, HttpFetch};

const FETCH_TIMEOUT_MS: u32 = 15_000;
const ACCESS_FILE: &str = "access.dsse.json";
const KEYS_FILE: &str = "keys.dsse.json";

const USAGE: &str = "\
Usage: cargo xtask access-seed --bootstrap-id <id> --trust-root <keyId=publicKey>... \\
           --mirror <url>... --out <file>
       cargo xtask access-seed --bootstrap-id <id> --trust-root <keyId=publicKey>... \\
           --verify <file>

  --bootstrap-id  the application's access.bootstrapId
  --trust-root    a root built into the app (repeatable): keyId=<base64url Ed25519 public key>
  --mirror        a bootstrap mirror base URL, as in bootstrap.sources (repeatable)
  --out           seed file to write (the DSSE envelope, verbatim)
  --verify        check an existing seed file instead of pulling a new one
  --ca-file       PEM of a private CA the mirrors' certificates chain to; the default is the public roots";

#[derive(Debug, Default, PartialEq, Eq)]
struct Options {
    bootstrap_id: String,
    trust_roots: Vec<TrustRootConfig>,
    mirrors: Vec<String>,
    out: Option<PathBuf>,
    verify: Option<PathBuf>,
    ca_file: Option<PathBuf>,
}

fn parse(args: &[String]) -> Result<Options> {
    let mut options = Options::default();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .cloned()
                .ok_or_else(|| anyhow!("{flag} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--bootstrap-id" => options.bootstrap_id = value()?,
            "--trust-root" => {
                let root = value()?;
                let (key_id, public_key) = root
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--trust-root expects keyId=publicKey, got {root}"))?;
                options.trust_roots.push(TrustRootConfig {
                    key_id: key_id.to_owned(),
                    public_key: public_key.to_owned(),
                });
            }
            "--mirror" => options.mirrors.push(value()?),
            "--out" => options.out = Some(PathBuf::from(value()?)),
            "--verify" => options.verify = Some(PathBuf::from(value()?)),
            "--ca-file" => options.ca_file = Some(PathBuf::from(value()?)),
            other => bail!("unknown option {other}\n{USAGE}"),
        }
    }
    if options.bootstrap_id.is_empty() || options.trust_roots.is_empty() {
        bail!("--bootstrap-id and at least one --trust-root are required\n{USAGE}");
    }
    let pulling = !options.mirrors.is_empty() && options.out.is_some();
    if pulling == options.verify.is_some() {
        bail!("give either --mirror with --out, or --verify\n{USAGE}");
    }
    Ok(options)
}

fn trust_store(options: &Options) -> Result<TrustStore> {
    let roots = TrustRoots::from_config(&options.trust_roots).context("invalid --trust-root")?;
    TrustStore::new(roots, &options.bootstrap_id).context("invalid --bootstrap-id")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

/// 能进安装包的清单：没有过期，并且列了引导源（种子的接入点下线时全靠它们自愈）。
fn check_shippable(manifest: &VerifiedAccessManifest, now_ms: i64) -> Result<()> {
    if manifest.is_stale(now_ms) {
        bail!(
            "manifest serial {} has already expired; publish a fresh one first",
            manifest.serial
        );
    }
    let sources = &manifest.manifest.bootstrap.sources;
    if !sources.iter().any(|source| source.kind == "https") {
        bail!(
            "manifest serial {} lists no https bootstrap source: a device could never recover",
            manifest.serial
        );
    }
    Ok(())
}

async fn download(http: &dyn HttpFetch, mirror: &str, file: &str) -> Result<Vec<u8>> {
    let url = format!("{}/{file}", mirror.trim_end_matches('/'));
    let response = http
        .fetch(&FetchRequest {
            url: url.clone(),
            timeout_ms: FETCH_TIMEOUT_MS,
            max_body_bytes: MAX_DOCUMENT_BYTES,
            ..FetchRequest::default()
        })
        .await
        .map_err(|error| anyhow!("{url}: {error}"))?;
    if response.status != 200 {
        bail!("{url}: HTTP {}", response.status);
    }
    Ok(response.body)
}

/// 从全部镜像取 serial 最大的已验签清单。先用各镜像里最新的密钥状态：
/// 被吊销的在线密钥签的清单不能写进种子。
async fn pull_newest(
    http: &dyn HttpFetch,
    mirrors: &[String],
    mut trust: TrustStore,
    now_ms: i64,
) -> Result<VerifiedAccessManifest> {
    for mirror in mirrors {
        let Ok(bytes) = download(http, mirror, KEYS_FILE).await else {
            continue;
        };
        match trust.verify_keystate(&bytes, now_ms) {
            Ok(keystate) => trust = trust.with_keystate(&keystate),
            Err(error) => eprintln!("access-seed: {mirror}: keystate rejected: {error}"),
        }
    }
    let mut newest: Option<VerifiedAccessManifest> = None;
    let mut failures = Vec::new();
    for mirror in mirrors {
        let verified = download(http, mirror, ACCESS_FILE).await.and_then(|bytes| {
            trust
                .verify_access(&bytes, now_ms)
                .map_err(|error| anyhow!("{mirror}: {error}"))
        });
        match verified {
            Ok(verified) => {
                println!("access-seed: {mirror}: serial {}", verified.serial);
                if newest
                    .as_ref()
                    .is_none_or(|known| verified.serial > known.serial)
                {
                    newest = Some(verified);
                }
            }
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    newest.ok_or_else(|| {
        anyhow!(
            "no mirror served a valid manifest:\n  {}",
            failures.join("\n  ")
        )
    })
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes).with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))
}

async fn pull(options: &Options, http: &dyn HttpFetch, now_ms: i64) -> Result<u64> {
    let out = options.out.as_deref().context("--out is required")?;
    let trust = trust_store(options)?;
    // 现有种子还验得过时不许倒退：镜像被回滚时构建应该失败，而不是悄悄打进旧地址。
    let existing = fs::read(out)
        .ok()
        .and_then(|bytes| trust.verify_access(&bytes, now_ms).ok());
    let newest = pull_newest(http, &options.mirrors, trust, now_ms).await?;
    check_shippable(&newest, now_ms)?;
    if let Some(existing) = existing
        && existing.serial > newest.serial
    {
        bail!(
            "mirrors serve serial {}, older than the existing seed {}",
            newest.serial,
            existing.serial
        );
    }
    write_atomically(out, &newest.envelope)?;
    println!(
        "access-seed: wrote serial {} ({}, signed by {}) to {}",
        newest.serial,
        newest.digest,
        newest.key_id,
        out.display()
    );
    Ok(newest.serial)
}

fn verify(options: &Options, now_ms: i64) -> Result<u64> {
    let path = options.verify.as_deref().context("--verify is required")?;
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let verified = trust_store(options)?
        .verify_access(&bytes, now_ms)
        .with_context(|| format!("{} does not verify", path.display()))?;
    check_shippable(&verified, now_ms)?;
    println!(
        "access-seed: {} is valid: serial {}, signed by {}",
        path.display(),
        verified.serial,
        verified.key_id
    );
    Ok(verified.serial)
}

pub fn run(args: &[String]) -> Result<()> {
    let options = parse(args)?;
    if options.verify.is_some() {
        return verify(&options, now_ms()).map(|_| ());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("start the async runtime")?;
    let http = match &options.ca_file {
        Some(file) => PlatformHttpFetch::new(
            SdkConfig {
                tls_ca_cert_path: Some(file.display().to_string()),
                ..SdkConfig::default()
            }
            .core_tls_config(),
        ),
        None => PlatformHttpFetch::default(),
    };
    runtime
        .block_on(pull(&options, &http, now_ms()))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use flare_im_core_sdk::link::flare_access::trust::sign::{
        Signer, SigningKey, encode_public_key, issue_key_cert, sign_access_manifest, sign_keystate,
    };
    use flare_im_core_sdk::link::flare_access::trust::{KeyCert, KeyScope, KeyState};
    use flare_im_core_sdk::link::flare_access::{FetchError, FetchResponse};
    use serde_json::{Value, json};

    use super::*;

    const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
    const MIRROR_A: &str = "https://mirror-a.example.com/flare/v1/b7f3c9a1e2d4b6a8/";
    const MIRROR_B: &str = "https://mirror-b.example.net/flare/v1/b7f3c9a1e2d4b6a8";
    /// 2026-11-04T08:00:00Z
    const NOW_MS: i64 = 1_793_779_200_000;

    fn root_key() -> SigningKey {
        SigningKey::from_bytes(&[0x11; 32])
    }

    fn online_key() -> SigningKey {
        SigningKey::from_bytes(&[0x33; 32])
    }

    fn manifest(serial: u64) -> Value {
        json!({
            "schema": "flare.access.v1",
            "bootstrapId": BOOTSTRAP_ID,
            "appId": "app_01J9ZK3X",
            "serial": serial,
            "issuedAt": "2026-11-04T07:00:00Z",
            "expiresAt": "2026-12-04T07:00:00Z",
            "regions": [{ "id": "cn-east", "cells": [{ "id": "cell-1", "accessPoints": [{
                "id": "ap-1",
                "host": "im.example.com",
                "status": "active",
                "services": { "imWs": { "path": "/ws" } }
            }]}]}],
            "placement": { "homeCell": "cell-1" },
            "bootstrap": { "sources": [{ "kind": "https", "url": MIRROR_A }] }
        })
    }

    fn signed(value: &Value) -> Vec<u8> {
        let root = root_key();
        let online = online_key();
        let cert = issue_key_cert(
            &Signer {
                key_id: "root-a",
                key: &root,
            },
            &KeyCert::new(
                "ok-1",
                encode_public_key(&online.verifying_key()),
                vec!["access".into()],
                KeyScope {
                    bootstrap_ids: vec!["*".into()],
                },
                "2026-07-01T00:00:00Z",
                "2026-12-28T00:00:00Z",
                "root-a",
            ),
        );
        sign_access_manifest(
            &serde_json::to_vec(value).unwrap(),
            &Signer {
                key_id: "ok-1",
                key: &online,
            },
            &[cert],
        )
    }

    #[derive(Default)]
    struct Mirrors(Mutex<HashMap<String, Vec<u8>>>);

    impl Mirrors {
        fn serve(&self, mirror: &str, file: &str, body: Vec<u8>) {
            self.0
                .lock()
                .unwrap()
                .insert(format!("{}/{file}", mirror.trim_end_matches('/')), body);
        }
    }

    #[async_trait::async_trait]
    impl HttpFetch for Mirrors {
        async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
            Ok(match self.0.lock().unwrap().get(&request.url) {
                Some(body) => FetchResponse {
                    status: 200,
                    etag: None,
                    body: body.clone(),
                },
                None => FetchResponse {
                    status: 404,
                    etag: None,
                    body: Vec::new(),
                },
            })
        }
    }

    fn options(out: &Path) -> Options {
        Options {
            bootstrap_id: BOOTSTRAP_ID.into(),
            trust_roots: vec![TrustRootConfig {
                key_id: "root-a".into(),
                public_key: encode_public_key(&root_key().verifying_key()),
            }],
            mirrors: vec![MIRROR_A.into(), MIRROR_B.into()],
            out: Some(out.to_path_buf()),
            verify: None,
            ca_file: None,
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flare-access-seed-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[tokio::test]
    async fn writes_the_newest_verified_manifest_verbatim() {
        let out = scratch("newest.seed");
        let _ = fs::remove_file(&out);
        let mirrors = Mirrors::default();
        mirrors.serve(MIRROR_A, ACCESS_FILE, signed(&manifest(10)));
        let newest = signed(&manifest(12));
        mirrors.serve(MIRROR_B, ACCESS_FILE, newest.clone());
        assert_eq!(pull(&options(&out), &mirrors, NOW_MS).await.unwrap(), 12);
        assert_eq!(fs::read(&out).unwrap(), newest);

        let mut check = options(&out);
        check.out = None;
        check.mirrors.clear();
        check.verify = Some(out.clone());
        assert_eq!(verify(&check, NOW_MS).unwrap(), 12);

        // 镜像回滚到更旧的版本：构建失败，种子不动。
        mirrors.serve(MIRROR_B, ACCESS_FILE, signed(&manifest(11)));
        let error = pull(&options(&out), &mirrors, NOW_MS).await.unwrap_err();
        assert!(error.to_string().contains("older than the existing seed"));
        assert_eq!(fs::read(&out).unwrap(), newest);
    }

    #[tokio::test]
    async fn refuses_what_should_not_ship() {
        let out = scratch("refused.seed");
        let _ = fs::remove_file(&out);

        // 只有被篡改的清单。
        let mirrors = Mirrors::default();
        let mut forged = manifest(10);
        let bytes = String::from_utf8(signed(&forged)).unwrap();
        forged["serial"] = 11.into();
        let payload = signed(&forged);
        let payload: Value = serde_json::from_slice(&payload).unwrap();
        let mut envelope: Value = serde_json::from_str(&bytes).unwrap();
        envelope["payload"] = payload["payload"].clone();
        mirrors.serve(MIRROR_A, ACCESS_FILE, envelope.to_string().into_bytes());
        let error = pull(&options(&out), &mirrors, NOW_MS).await.unwrap_err();
        assert!(
            error.to_string().contains("no mirror served a valid"),
            "{error}"
        );

        // 被密钥状态吊销的在线密钥。
        let root = root_key();
        let keystate = KeyState {
            schema: "flare.keystate.v1".into(),
            serial: 1,
            issued_at: "2026-11-04T00:00:00Z".into(),
            certificates: vec![],
            revoked_key_ids: vec!["ok-1".into()],
        };
        let mirrors = Mirrors::default();
        mirrors.serve(
            MIRROR_A,
            KEYS_FILE,
            sign_keystate(
                &keystate,
                &Signer {
                    key_id: "root-a",
                    key: &root,
                },
            ),
        );
        mirrors.serve(MIRROR_B, ACCESS_FILE, signed(&manifest(10)));
        assert!(pull(&options(&out), &mirrors, NOW_MS).await.is_err());

        // 没有引导源。
        let mut no_sources = manifest(10);
        no_sources["bootstrap"]["sources"] = json!([]);
        let mirrors = Mirrors::default();
        mirrors.serve(MIRROR_A, ACCESS_FILE, signed(&no_sources));
        let error = pull(&options(&out), &mirrors, NOW_MS).await.unwrap_err();
        assert!(
            error.to_string().contains("no https bootstrap source"),
            "{error}"
        );

        // 已过期。
        let mirrors = Mirrors::default();
        mirrors.serve(MIRROR_A, ACCESS_FILE, signed(&manifest(10)));
        let after_expiry = NOW_MS + 31 * 24 * 3_600_000;
        let error = pull(&options(&out), &mirrors, after_expiry)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("expired"), "{error}");

        assert!(!out.exists());
    }

    #[test]
    fn arguments_are_validated() {
        let args = |list: &[&str]| list.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
        let parsed = parse(&args(&[
            "--bootstrap-id",
            BOOTSTRAP_ID,
            "--trust-root",
            "root-a=AAAA",
            "--mirror",
            MIRROR_A,
            "--out",
            "seed.json",
        ]))
        .unwrap();
        assert_eq!(parsed.trust_roots[0].key_id, "root-a");
        assert_eq!(parsed.out, Some(PathBuf::from("seed.json")));
        assert_eq!(parsed.ca_file, None);
        let private_ca = parse(&args(&[
            "--bootstrap-id",
            BOOTSTRAP_ID,
            "--trust-root",
            "root-a=AAAA",
            "--mirror",
            MIRROR_A,
            "--out",
            "seed.json",
            "--ca-file",
            "ca.pem",
        ]))
        .unwrap();
        assert_eq!(private_ca.ca_file, Some(PathBuf::from("ca.pem")));
        assert!(parse(&args(&["--bootstrap-id", BOOTSTRAP_ID])).is_err());
        assert!(
            parse(&args(&[
                "--bootstrap-id",
                BOOTSTRAP_ID,
                "--trust-root",
                "root-a=AAAA",
                "--mirror",
                MIRROR_A,
            ]))
            .is_err()
        );
        assert!(parse(&args(&["--wat"])).is_err());
    }
}
