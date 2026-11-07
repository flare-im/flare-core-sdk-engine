//! 实验台的引导面：本机假镜像（真实 HTTP，带 ETag 与 304）、测试根与在线密钥、生产模式的接入配置。
//!
//! 清单里的主机名与 443 端口在本机连不到，所以网络的两端由 [`AccessLab`] 换掉：拉取经 [`LabNet`] 转到假镜像，
//! IM 入口改写到接入点的回环中继。其余走的都是生产路径：验签、防回滚、LKG、选路、链路状态机。

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use flare_im_core_sdk::link::flare_access::store::MemoryAccessStore;
use flare_im_core_sdk::link::flare_access::trust::sign::{
    Signer, SigningKey, encode_public_key, issue_key_cert, sign_access_manifest,
};
use flare_im_core_sdk::link::flare_access::trust::{KeyCert, KeyScope, TrustRootConfig};
use flare_im_core_sdk::link::flare_access::{FetchError, FetchRequest, FetchResponse, HttpFetch};
use flare_im_core_sdk::link::{AccessLab, AccessRuntime, AccessStoreSource, PlatformHttpFetch};
use flare_im_core_sdk::prelude::AccessConfig;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::{AccessPoint, drill_policy};

pub const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
pub const ACCESS_FILE: &str = "access.dsse.json";

fn rfc3339(offset: ChronoDuration) -> String {
    (Utc::now() + offset).to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn etag_of(body: &[u8]) -> String {
    let mut hasher = DefaultHasher::new();
    body.hash(&mut hasher);
    format!("\"{:x}\"", hasher.finish())
}

/// 一个镜像：按文件名回文档，`If-None-Match` 命中回 304。
pub struct Mirror {
    pub host: String,
    pub address: String,
    documents: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    requests: Arc<AtomicUsize>,
    accept_loop: Mutex<Option<JoinHandle<()>>>,
}

impl Mirror {
    pub async fn start(host: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mirror");
        let address = listener.local_addr().unwrap().to_string();
        let documents: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::default();
        let requests = Arc::new(AtomicUsize::new(0));
        let accept_loop = tokio::spawn({
            let documents = documents.clone();
            let requests = requests.clone();
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(answer(stream, documents.clone(), requests.clone()));
                }
            }
        });
        Self {
            host: host.to_owned(),
            address,
            documents,
            requests,
            accept_loop: Mutex::new(Some(accept_loop)),
        }
    }

    pub fn base_url(&self) -> String {
        format!("https://{}/flare/v1/{BOOTSTRAP_ID}/", self.host)
    }

    pub fn publish(&self, file: &str, body: Vec<u8>) {
        self.documents.lock().unwrap().insert(file.to_owned(), body);
    }

    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// 这个镜像不可用：不再接受连接。
    pub fn take_down(&self) {
        if let Some(accept_loop) = self.accept_loop.lock().unwrap().take() {
            accept_loop.abort();
        }
    }
}

impl Drop for Mirror {
    fn drop(&mut self) {
        self.take_down();
    }
}

async fn answer(
    mut stream: TcpStream,
    documents: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    requests: Arc<AtomicUsize>,
) {
    let mut request = Vec::new();
    let mut buffer = [0u8; 2_048];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    requests.fetch_add(1, Ordering::SeqCst);
    let head = String::from_utf8_lossy(&request).into_owned();
    let path = head.split_whitespace().nth(1).unwrap_or_default();
    let file = path.rsplit('/').next().unwrap_or_default();
    let condition = head.lines().find_map(|line| {
        line.to_ascii_lowercase()
            .starts_with("if-none-match:")
            .then(|| line["if-none-match:".len()..].trim().to_owned())
    });
    let document = documents.lock().unwrap().get(file).cloned();
    let response = match document {
        Some(body) => {
            let etag = etag_of(&body);
            if condition.as_deref() == Some(etag.as_str()) {
                format!("HTTP/1.1 304 Not Modified\r\netag: {etag}\r\nconnection: close\r\n\r\n")
                    .into_bytes()
            } else {
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\netag: {etag}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                response.extend_from_slice(&body);
                response
            }
        }
        None => {
            b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec()
        }
    };
    let _ = stream.write_all(&response).await;
    let _ = stream.shutdown().await;
}

/// 把 `https://{镜像主机}/…` 转到本机假镜像，走真实的 HTTP 栈。不认识的主机一律不可达。
pub struct LabNet {
    mirrors: HashMap<String, String>,
    http: PlatformHttpFetch,
}

#[async_trait::async_trait]
impl HttpFetch for LabNet {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        let url = url::Url::parse(&request.url).map_err(|_| FetchError::Unreachable)?;
        let host = url.host_str().unwrap_or_default();
        let address = self.mirrors.get(host).ok_or(FetchError::Unreachable)?;
        let mut forwarded = request.clone();
        forwarded.url = format!("http://{address}{}", url.path());
        self.http.fetch(&forwarded).await
    }
}

/// 接入点在清单里的主机名。
pub fn ap_host(access_point: &AccessPoint) -> String {
    format!("{}.lab.example", access_point.id)
}

/// 实验台的信任根与在线密钥。
pub struct Authority {
    root: SigningKey,
    online: SigningKey,
    sources: Vec<String>,
}

impl Authority {
    pub fn new(mirrors: &[&Mirror]) -> Self {
        Self {
            root: SigningKey::from_bytes(&[0x11; 32]),
            online: SigningKey::from_bytes(&[0x33; 32]),
            sources: mirrors.iter().map(|mirror| mirror.base_url()).collect(),
        }
    }

    pub fn manifest(&self, serial: u64, access_points: &[&AccessPoint]) -> Value {
        let access_points: Vec<Value> = access_points
            .iter()
            .map(|access_point| {
                json!({
                    "id": access_point.id,
                    "host": ap_host(access_point),
                    "status": "active",
                    "services": { "imWs": { "path": "/ws" } }
                })
            })
            .collect();
        let sources: Vec<Value> = self
            .sources
            .iter()
            .map(|url| json!({ "kind": "https", "url": url }))
            .collect();
        json!({
            "schema": "flare.access.v1",
            "bootstrapId": BOOTSTRAP_ID,
            "appId": "app_lab",
            "serial": serial,
            "issuedAt": rfc3339(ChronoDuration::hours(-1)),
            "expiresAt": rfc3339(ChronoDuration::days(30)),
            "regions": [{ "id": "region-lab", "cells": [{
                "id": "cell-lab",
                "accessPoints": access_points
            }]}],
            "placement": { "homeCell": "cell-lab" },
            "bootstrap": { "sources": sources },
            "clientPolicy": drill_policy()
        })
    }

    pub fn sign(&self, manifest: &Value) -> Vec<u8> {
        let cert = issue_key_cert(
            &Signer {
                key_id: "root-lab",
                key: &self.root,
            },
            &KeyCert::new(
                "ok-lab",
                encode_public_key(&self.online.verifying_key()),
                vec!["access".into()],
                KeyScope {
                    bootstrap_ids: vec!["*".into()],
                },
                rfc3339(ChronoDuration::days(-10)),
                rfc3339(ChronoDuration::days(100)),
                "root-lab",
            ),
        );
        sign_access_manifest(
            &serde_json::to_vec(manifest).unwrap(),
            &Signer {
                key_id: "ok-lab",
                key: &self.online,
            },
            &[cert],
        )
    }

    /// 生产模式的接入配置：种子是这份已签名清单。
    pub fn access(&self, seed: &Value) -> AccessConfig {
        AccessConfig::production(
            BOOTSTRAP_ID,
            vec![TrustRootConfig {
                key_id: "root-lab".into(),
                public_key: encode_public_key(&self.root.verifying_key()),
            }],
            Some(String::from_utf8(self.sign(seed)).unwrap()),
        )
    }
}

/// 生产模式的接入运行时，`store` 模拟设备上的 LKG（跨「重启」共用）。
pub fn lab_runtime(
    access: AccessConfig,
    store: &Arc<MemoryAccessStore>,
    lab: AccessLab,
) -> Arc<AccessRuntime> {
    Arc::new(
        AccessRuntime::new(Some(access), AccessStoreSource::Ready(store.clone())).with_lab(lab),
    )
}

/// 换掉网络两端：镜像主机名 → 本机假镜像；`wss://{接入点主机}/ws` → 接入点的回环中继。
pub fn lab_network(mirrors: &[&Mirror], access_points: &[&AccessPoint]) -> AccessLab {
    let net = LabNet {
        mirrors: mirrors
            .iter()
            .map(|mirror| (mirror.host.clone(), mirror.address.clone()))
            .collect(),
        http: PlatformHttpFetch::default(),
    };
    let dial: HashMap<String, String> = access_points
        .iter()
        .map(|access_point| {
            (
                format!("wss://{}/ws", ap_host(access_point)),
                access_point.ws_url(),
            )
        })
        .collect();
    AccessLab {
        http: Arc::new(net),
        dial: Arc::new(move |url: &str| dial.get(url).cloned().unwrap_or_else(|| url.to_owned())),
    }
}
