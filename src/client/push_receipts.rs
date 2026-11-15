//! 推送到达回执：这台设备收到了哪条推送，报给服务端，算到达率、补全「为什么没提示」的决策链。
//!
//! 回执有两个来源：
//!
//! - 应用进程处理了一条推送（`handleRemoteMessage`），路径 `app`；
//! - iOS 通知扩展收到了推送，往与主应用共用的目录里的 `receipts.log` 追加一行，路径 `extension`。
//!   会话开始、回到前台、后台刷新之后读进来：先改名成 `receipts.log.reading`，读完删掉，扩展照样往
//!   新的 `receipts.log` 里追加。每行带推送发给的账号（`u` 用户、`n` 租户）：不是当前会话这个账号的行
//!   （包括没写账号的）丢掉，不记到正好登录着的人头上。
//!
//! 按（合并键、seq、通道）去重后排进队列，队列存在数据根下的 `push-receipts.json`，最多 500 条，满了丢
//! 最旧的。会话装上后批量上报（`POST /api/v1/push/receipts`，每批最多 100 条）：4xx 整批丢弃，5xx 与网络
//! 错误留着下次再报；推送唤醒补完同步之后报一次，没有别的时机时最多 30 秒报一次。每批用会话当前的接入
//! 凭据（刷新、续期之后的那枚），和别的 Core 请求一样：答凭据过期时续期一次再发，续期也失败才留着下次再报。
//!
//! 回执只是尽力而为的度量，丢了不影响投递。里面只有会话、消息的标识、处置与时间，没有内容、令牌与
//! 凭据，日志里也不打。浏览器没有系统推送：wasm 构建里这里什么都不做。

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::{PushReceipts, ReceiptSender};
#[cfg(target_arch = "wasm32")]
pub(crate) use stub::{PushReceipts, ReceiptSender};

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::collections::{BTreeMap, HashSet, VecDeque};
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use crate::infrastructure::transport::http::http_client::is_core_bearer_expired;
    use crate::infrastructure::transport::http::{HttpApiResponse, HttpClient};
    use crate::shared::error::FlareError;
    use crate::shared::util::atomic_file::{remove_if_present, write_atomic};
    use crate::shared::util::{delay, normalize_tenant_id, now_millis, spawn_background, timeout};

    /// 上报的路径（与推送令牌同一个 Core API 网关）。
    const RECEIPTS_PATH: &str = "/api/v1/push/receipts";
    /// 队列文件（在数据根下）。
    const QUEUE_FILE: &str = "push-receipts.json";
    /// 扩展追加回执的日志（在与扩展共用的目录下）。
    const EXTENSION_LOG: &str = "receipts.log";
    /// 读的时候先改名成它：扩展照样往新的日志里追加，读到一半的不会和新写的混在一起。
    const EXTENSION_LOG_READING: &str = "receipts.log.reading";
    /// 队列最多多少条，满了丢最旧的。
    const MAX_QUEUED: usize = 500;
    /// 报过的记住多少条：扩展与应用进程都记了同一条推送时只报一次。
    const MAX_REPORTED: usize = 500;
    /// 每批最多多少条。
    const BATCH: usize = 100;
    /// 一次上报最多发几批：其余的等下一次。
    const MAX_BATCHES: usize = 5;
    /// 没有别的时机时两次上报至少隔多久。
    const FLUSH_GAP_MS: i64 = 30_000;
    /// 一次请求的时限。
    const REQUEST_CAP: Duration = Duration::from_secs(10);
    /// 读扩展日志、队列文件的大小上限（扩展自己在 64 KiB 处停止追加）。
    const MAX_FILE_BYTES: u64 = 256 * 1024;
    /// 键与标识的长度上限：扩展写的日志不可信，超长的行当作坏行。
    const MAX_FIELD_LEN: usize = 256;
    const QUEUE_VERSION: u32 = 1;

    /// 一条回执，线上的形状（JSON 驼峰）。
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Receipt {
        conversation_id: String,
        message_id: String,
        seq: u64,
        collapse_id: String,
        disposition: String,
        provider: String,
        path: String,
        received_at_ms: i64,
    }

    impl Receipt {
        fn key(&self) -> (String, u64, String) {
            (self.collapse_id.clone(), self.seq, self.provider.clone())
        }
    }

    /// 队列里的一条：回执属于哪个账号（只用这个账号的会话报，不上报）。
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Queued {
        user_id: String,
        tenant_id: String,
        receipt: Receipt,
    }

    #[derive(Debug, Default, Serialize, Deserialize)]
    struct QueueFile {
        v: u32,
        #[serde(default)]
        queue: Vec<Queued>,
    }

    /// 扩展写的一行。
    #[derive(Debug, Deserialize)]
    struct ExtensionLine {
        /// 推送发给的用户（载荷 `uid`）。
        #[serde(default)]
        u: String,
        /// 推送发给的租户（载荷 `tenant_id`）。
        #[serde(default)]
        n: String,
        c: String,
        #[serde(default)]
        m: String,
        #[serde(default)]
        s: u64,
        k: String,
        #[serde(default)]
        d: String,
        p: String,
        #[serde(default)]
        t: i64,
    }

    #[derive(Deserialize)]
    struct Accepted {
        #[serde(default)]
        #[allow(dead_code)]
        accepted: u32,
    }

    /// 会话装上之后才有的上报通道：设备、会话共用的 Core API 客户端（凭据每批现取，见 [`send_batch`]）、
    /// 这个会话的账号，以及它属于哪一代会话。
    #[derive(Clone)]
    pub(crate) struct ReceiptSender {
        pub device_id: String,
        /// 共享请求上下文的 Core API 客户端：上下文里的凭据随刷新、续期更新。
        pub http: HttpClient,
        pub user_id: String,
        pub tenant_id: String,
        pub epoch: u64,
        pub generation: Arc<AtomicU64>,
    }

    impl ReceiptSender {
        fn current(&self) -> bool {
            self.generation.load(Ordering::Acquire) == self.epoch
        }
    }

    #[derive(Default)]
    struct State {
        /// 数据根；队列文件在它下面。
        root: Option<PathBuf>,
        queue: VecDeque<Queued>,
        /// 报过的（或被服务端拒掉的），按先后。
        reported: VecDeque<(String, u64, String)>,
        /// 当前会话的账号：记录与上报都只认它。
        owner: Option<(String, String)>,
        sender: Option<ReceiptSender>,
        /// 最近一次登记推送令牌时带的应用版本。
        app_version: Option<String>,
        last_flush_ms: i64,
        flush_scheduled: bool,
    }

    impl State {
        fn known(&self, key: &(String, u64, String)) -> bool {
            self.reported.contains(key)
                || self.queue.iter().any(|queued| queued.receipt.key() == *key)
        }

        fn push(&mut self, user_id: &str, tenant_id: &str, receipt: Receipt) -> bool {
            if self.known(&receipt.key()) {
                return false;
            }
            while self.queue.len() >= MAX_QUEUED {
                self.queue.pop_front();
            }
            self.queue.push_back(Queued {
                user_id: user_id.to_owned(),
                tenant_id: tenant_id.to_owned(),
                receipt,
            });
            true
        }

        fn note_reported(&mut self, key: (String, u64, String)) {
            while self.reported.len() >= MAX_REPORTED {
                self.reported.pop_front();
            }
            self.reported.push_back(key);
        }

        /// 队列文件的内容；没有数据根时为 `None`。
        fn snapshot(&self) -> Option<(PathBuf, Vec<u8>)> {
            let root = self.root.as_ref()?;
            let file = QueueFile {
                v: QUEUE_VERSION,
                queue: self.queue.iter().cloned().collect(),
            };
            Some((root.join(QUEUE_FILE), serde_json::to_vec(&file).ok()?))
        }
    }

    /// 一批上报的结果。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Sent {
        /// 服务端收下了。
        Accepted,
        /// 服务端拒掉了（4xx、业务错误）：再报也一样，丢掉。
        Rejected,
        /// 服务端或网络出了问题（5xx、超时、连不上、凭据过期）：留着下次再报。
        Retry,
    }

    /// 推送到达回执的队列与上报。跨登录保留（队列在数据根下），上报只用当前会话的凭据。
    #[derive(Default)]
    pub(crate) struct PushReceipts {
        state: Mutex<State>,
        /// 同一时间只有一次上报。
        flushing: tokio::sync::Mutex<()>,
        /// 写队列文件排成一队：后写的总是更新的那份。
        saving: Mutex<()>,
    }

    impl PushReceipts {
        fn lock(&self) -> MutexGuard<'_, State> {
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        /// 数据根（准备会话时）：换了数据根就从那里读出排着的回执。
        pub(crate) fn set_root(&self, root: PathBuf) {
            let mut state = self.lock();
            if state.root.as_deref() == Some(root.as_path()) {
                return;
            }
            let loaded = read_bounded(&root.join(QUEUE_FILE))
                .and_then(|bytes| serde_json::from_slice::<QueueFile>(&bytes).ok())
                .filter(|file| file.v == QUEUE_VERSION)
                .map(|file| file.queue)
                .unwrap_or_default();
            state.root = Some(root);
            state.queue.clear();
            for queued in loaded {
                let Queued {
                    user_id,
                    tenant_id,
                    receipt,
                } = queued;
                state.push(&user_id, &tenant_id, receipt);
            }
        }

        /// 当前会话的账号（准备会话、建连时）。别的账号排着的回执用这个会话报不了，丢掉。
        pub(crate) fn set_owner(&self, user_id: &str, tenant_id: &str) {
            let changed = {
                let mut state = self.lock();
                let owner = (user_id.to_owned(), tenant_id.to_owned());
                if state.owner.as_ref() == Some(&owner) {
                    false
                } else {
                    let before = state.queue.len();
                    state
                        .queue
                        .retain(|queued| queued.user_id == owner.0 && queued.tenant_id == owner.1);
                    state.owner = Some(owner);
                    state.queue.len() != before
                }
            };
            if changed {
                self.save();
            }
        }

        /// 会话装上了上报通道（建连之后）。
        pub(crate) fn attach(&self, sender: ReceiptSender) {
            self.lock().sender = Some(sender);
        }

        /// 会话结束（登出、断开）：不再记、不再报；排着的留在文件里，下次同一账号登录再报。
        pub(crate) fn detach(&self) {
            let mut state = self.lock();
            state.sender = None;
            state.owner = None;
        }

        /// 最近一次登记推送令牌时带的应用版本。
        pub(crate) fn set_app_version(&self, app_version: Option<String>) {
            self.lock().app_version = app_version;
        }

        /// 应用进程处理了一条属于当前账号的推送（路径 `app`）。
        pub(crate) fn record_app(
            self: &Arc<Self>,
            provider: &str,
            payload: &BTreeMap<String, String>,
        ) {
            let text = |key: &str| {
                payload
                    .get(key)
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty() && value.len() <= MAX_FIELD_LEN)
                    .unwrap_or_default()
                    .to_owned()
            };
            let receipt = Receipt {
                conversation_id: text("conversation_id"),
                message_id: text("message_id"),
                seq: text("seq").parse().unwrap_or(0),
                collapse_id: text("collapse_id"),
                disposition: text("disposition"),
                provider: provider.to_owned(),
                path: "app".to_owned(),
                received_at_ms: now_millis() as i64,
            };
            if receipt.collapse_id.is_empty() || receipt.conversation_id.is_empty() {
                return;
            }
            let queued = {
                let mut state = self.lock();
                let Some((user_id, tenant_id)) = state.owner.clone() else {
                    return;
                };
                state.push(&user_id, &tenant_id, receipt)
            };
            if queued {
                let receipts = self.clone();
                // 写队列文件放到后台：推送入口要在 150 ms 内答复。
                spawn_background(async move { receipts.save() });
                self.schedule_flush();
            }
        }

        /// 读进扩展记下的回执（路径 `extension`）：改名、读、删。返回读进来的条数。
        pub(crate) fn ingest(&self, directory: &Path) -> usize {
            let log = directory.join(EXTENSION_LOG);
            let reading = directory.join(EXTENSION_LOG_READING);
            // 上一次读到一半（进程被杀）留下的先读掉，再把新的改名过来。
            let mut bytes = read_bounded(&reading).unwrap_or_default();
            if std::fs::rename(&log, &reading).is_ok() {
                bytes.extend(read_bounded(&reading).unwrap_or_default());
            }
            if let Err(error) = remove_if_present(&reading) {
                tracing::debug!(%error, "removing the extension receipt log failed");
            }
            if bytes.is_empty() {
                return 0;
            }
            let ingested = {
                let mut state = self.lock();
                let Some((user_id, tenant_id)) = state.owner.clone() else {
                    return 0;
                };
                parse_extension_log(&bytes, &user_id, &tenant_id)
                    .into_iter()
                    .filter(|receipt| state.push(&user_id, &tenant_id, receipt.clone()))
                    .count()
            };
            if ingested > 0 {
                self.save();
            }
            ingested
        }

        /// 有新回执时登记一次上报：离上一次不到 30 秒就等到 30 秒。
        fn schedule_flush(self: &Arc<Self>) {
            let wait_ms = {
                let mut state = self.lock();
                if state.flush_scheduled || state.sender.is_none() {
                    return;
                }
                state.flush_scheduled = true;
                (state.last_flush_ms + FLUSH_GAP_MS - now_millis() as i64).max(0)
            };
            let receipts = Arc::downgrade(self);
            spawn_background(async move {
                delay(Duration::from_millis(wait_ms as u64)).await;
                if let Some(receipts) = receipts.upgrade() {
                    receipts.lock().flush_scheduled = false;
                    receipts.flush().await;
                }
            });
        }

        /// 报一次：当前账号排着的回执按批发出去。已有一次在报、没有上报通道时什么都不做。
        pub(crate) async fn flush(&self) {
            let Ok(_flushing) = self.flushing.try_lock() else {
                return;
            };
            for _ in 0..MAX_BATCHES {
                let (sender, batch, app_version) = {
                    let state = self.lock();
                    let (Some(sender), Some((user_id, tenant_id))) =
                        (state.sender.clone(), state.owner.clone())
                    else {
                        return;
                    };
                    if !sender.current() {
                        return;
                    }
                    let batch: Vec<Receipt> = state
                        .queue
                        .iter()
                        .filter(|queued| queued.user_id == user_id && queued.tenant_id == tenant_id)
                        .take(BATCH)
                        .map(|queued| queued.receipt.clone())
                        .collect();
                    if batch.is_empty() {
                        return;
                    }
                    (sender, batch, state.app_version.clone().unwrap_or_default())
                };
                let body = serde_json::json!({
                    "deviceId": sender.device_id,
                    "appVersion": app_version,
                    "receipts": batch,
                });
                // 会话换代了、或手上没有凭据：这批留着，下次同一账号的会话再报。
                let Some(sent) = send_batch(&sender, &body).await else {
                    return;
                };
                {
                    let mut state = self.lock();
                    state.last_flush_ms = now_millis() as i64;
                    if sent == Sent::Retry {
                        tracing::debug!(
                            count = batch.len(),
                            "push receipts kept for a later report"
                        );
                        return;
                    }
                    if sent == Sent::Rejected {
                        tracing::debug!(count = batch.len(), "push receipts rejected; dropped");
                    }
                    let keys: HashSet<_> = batch.iter().map(Receipt::key).collect();
                    state
                        .queue
                        .retain(|queued| !keys.contains(&queued.receipt.key()));
                    for key in keys {
                        state.note_reported(key);
                    }
                }
                self.save();
            }
        }

        /// 把队列写回数据根。
        fn save(&self) {
            let _saving = self
                .saving
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some((path, bytes)) = self.lock().snapshot() else {
                return;
            };
            if let Err(error) = write_atomic(&path, &bytes) {
                tracing::debug!(%error, "saving the push receipt queue failed");
            }
        }
    }

    /// 用会话当前的接入凭据发一批。
    ///
    /// 凭据每批从共享请求上下文现取（令牌刷新、宿主递来新令牌、别的请求续期之后都是新的那枚），钉在这一次
    /// 请求上，绝不借用下一个账号的：先取凭据、再核对这一代会话还在。登出、换账号时总是先换代、后换上下文里
    /// 的凭据，所以核对通过时取到的一定是这个账号的。
    ///
    /// Core 答凭据过期（401 `TOKEN_EXPIRED`）时和别的 Core 请求一样续期一次：上下文里已经换了新的就用它，
    /// 否则走上下文的续期；换到了、会话也还在，就用新的再发一次。续期没换到新的（刷新确实失败了），或答的是
    /// 别的 401（和别的 Core 请求一样不续期），才按 401 留着下次再报。会话已经换代、或上下文里没有凭据时为
    /// `None`（这批不动）。
    async fn send_batch(sender: &ReceiptSender, body: &serde_json::Value) -> Option<Sent> {
        let mut bearer = sender.http.current_core_bearer().await?;
        let mut renewed = false;
        loop {
            if !sender.current() {
                return None;
            }
            let http = sender
                .http
                .pinned_core_auth(bearer.clone(), &sender.user_id, &sender.tenant_id)
                .await;
            let response = timeout(
                REQUEST_CAP,
                http.post::<_, HttpApiResponse<Accepted>>(RECEIPTS_PATH, body),
            )
            .await;
            let error = match response {
                Ok(Ok(response)) if response.is_success() => return Some(Sent::Accepted),
                Ok(Ok(_)) => return Some(Sent::Rejected),
                Ok(Err(error)) => error,
                Err(_) => return Some(Sent::Retry),
            };
            if renewed || !is_core_bearer_expired(&error) {
                return Some(classify_error(&error));
            }
            renewed = true;
            let fresh = match sender
                .http
                .current_core_bearer()
                .await
                .filter(|current| *current != bearer)
            {
                Some(current) => Some(current),
                None => sender.http.renew_core_bearer(&bearer).await,
            };
            match fresh {
                Some(fresh) => bearer = fresh,
                None => return Some(classify_error(&error)),
            }
        }
    }

    /// 4xx 是这批回执本身的问题，丢掉；凭据过期（401，续期也没换到新的）、超时（408）、限流（429）、5xx 与
    /// 网络错误下次再报。
    fn classify_error(error: &FlareError) -> Sent {
        let status = error
            .as_localized()
            .and_then(|error| error.params)
            .and_then(|params| params.get("httpStatus").cloned())
            .and_then(|status| status.parse::<u16>().ok());
        match status {
            Some(401 | 408 | 429) => Sent::Retry,
            Some(400..=499) => Sent::Rejected,
            _ => Sent::Retry,
        }
    }

    /// 读一个不太大的文件；不存在、读不了、超过上限时为 `None`。
    fn read_bounded(path: &Path) -> Option<Vec<u8>> {
        let file = std::fs::File::open(path).ok()?;
        let mut bytes = Vec::new();
        file.take(MAX_FILE_BYTES + 1).read_to_end(&mut bytes).ok()?;
        (bytes.len() as u64 <= MAX_FILE_BYTES).then_some(bytes)
    }

    /// 扩展日志：一行一个 JSON 对象。坏行、缺键、认不得的通道跳过；推送发给的不是这个账号（`u` 用户、
    /// `n` 租户与当前会话不同，或没写）的行丢掉：别人的推送不记到正好登录着的人头上。租户与推送入口认载荷时
    /// 一样比：没写的不算，`default` 按默认租户算。
    fn parse_extension_log(bytes: &[u8], user_id: &str, tenant_id: &str) -> Vec<Receipt> {
        let field = |value: &str| {
            let value = value.trim();
            (!value.is_empty() && value.len() <= MAX_FIELD_LEN).then(|| value.to_owned())
        };
        let owned = |line: &ExtensionLine| {
            field(&line.u).is_some_and(|user| user == user_id)
                && field(&line.n).is_some_and(|tenant| normalize_tenant_id(tenant) == tenant_id)
        };
        bytes
            .split(|byte| *byte == b'\n')
            .filter_map(|line| serde_json::from_slice::<ExtensionLine>(line).ok())
            .filter(owned)
            .filter_map(|line| {
                let provider = field(&line.p)
                    .filter(|provider| matches!(provider.as_str(), "apns" | "fcm" | "getui"))?;
                Some(Receipt {
                    conversation_id: field(&line.c)?,
                    message_id: field(&line.m).unwrap_or_default(),
                    seq: line.s,
                    collapse_id: field(&line.k)?,
                    disposition: field(&line.d).unwrap_or_default(),
                    provider,
                    path: "extension".to_owned(),
                    received_at_ms: line.t,
                })
            })
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn temp_dir() -> PathBuf {
            let dir = std::env::temp_dir().join(format!("flare-receipts-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        fn payload(collapse: &str, seq: &str) -> BTreeMap<String, String> {
            [
                ("conversation_id", "c-1"),
                ("message_id", "m-9"),
                ("seq", seq),
                ("collapse_id", collapse),
                ("disposition", "loud"),
                ("title", "Bob"),
                ("body", "hello"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
        }

        /// 应用进程的回执：只记当前账号的，按（合并键、seq、通道）去重；正文不进回执。
        #[tokio::test]
        async fn app_receipts_are_owned_and_deduplicated() {
            let receipts = Arc::new(PushReceipts::default());
            receipts.record_app("fcm", &payload("c:1", "5"));
            assert!(receipts.lock().queue.is_empty(), "没有会话不记");
            receipts.set_owner("alice", "0");
            receipts.record_app("fcm", &payload("c:1", "5"));
            receipts.record_app("fcm", &payload("c:1", "5"));
            receipts.record_app("getui", &payload("c:1", "5"));
            receipts.record_app("fcm", &payload("c:1", "6"));
            let state = receipts.lock();
            assert_eq!(state.queue.len(), 3);
            let first = &state.queue[0].receipt;
            assert_eq!(first.path, "app");
            assert_eq!(first.seq, 5);
            assert_eq!(first.message_id, "m-9");
            let wire = serde_json::to_value(first).unwrap();
            assert!(wire.get("collapseId").is_some() && wire.get("receivedAtMs").is_some());
            assert!(!wire.to_string().contains("hello"), "正文不进回执");
        }

        /// 扩展日志：改名、读、删；坏行与认不得的通道跳过；与已有的去重。
        #[test]
        fn the_extension_log_is_ingested_once() {
            let dir = temp_dir();
            std::fs::write(
                dir.join(EXTENSION_LOG),
                concat!(
                    r#"{"u":"alice","n":"0","c":"c-1","m":"m-9","s":123,"k":"c:abc","d":"loud","p":"apns","t":1731400000000}"#,
                    "\n",
                    "not json\n",
                    r#"{"u":"alice","n":"0","c":"c-1","s":1,"k":"c:abc","p":"webpush","t":1}"#,
                    "\n",
                    r#"{"u":"alice","n":"0","c":"c-1","m":"m-9","s":123,"k":"c:abc","d":"loud","p":"apns","t":1731400000001}"#,
                    "\n",
                ),
            )
            .unwrap();
            let receipts = PushReceipts::default();
            receipts.set_owner("alice", "0");
            assert_eq!(receipts.ingest(&dir), 1);
            assert!(!dir.join(EXTENSION_LOG).exists());
            assert!(!dir.join(EXTENSION_LOG_READING).exists());
            assert_eq!(receipts.lock().queue[0].receipt.path, "extension");
            assert_eq!(receipts.ingest(&dir), 0);
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// 扩展日志里推送发给别的账号的行丢掉，没写账号的也丢掉；`default` 租户按默认租户算。
        #[test]
        fn extension_lines_of_another_account_are_dropped() {
            let dir = temp_dir();
            std::fs::write(
                dir.join(EXTENSION_LOG),
                concat!(
                    r#"{"u":"alice","n":"0","c":"c-1","s":1,"k":"c:1","d":"loud","p":"apns","t":1}"#,
                    "\n",
                    r#"{"u":"bob","n":"0","c":"c-1","s":2,"k":"c:2","d":"loud","p":"apns","t":2}"#,
                    "\n",
                    r#"{"u":"alice","n":"7","c":"c-1","s":3,"k":"c:3","d":"loud","p":"apns","t":3}"#,
                    "\n",
                    r#"{"c":"c-1","s":4,"k":"c:4","d":"loud","p":"apns","t":4}"#,
                    "\n",
                    r#"{"u":"","n":"0","c":"c-1","s":5,"k":"c:5","d":"loud","p":"apns","t":5}"#,
                    "\n",
                    r#"{"u":" alice ","n":"default","c":"c-1","s":6,"k":"c:6","d":"loud","p":"apns","t":6}"#,
                    "\n",
                ),
            )
            .unwrap();
            let receipts = PushReceipts::default();
            receipts.set_owner("alice", "0");
            assert_eq!(receipts.ingest(&dir), 2);
            let state = receipts.lock();
            let seqs: Vec<u64> = state
                .queue
                .iter()
                .map(|queued| queued.receipt.seq)
                .collect();
            assert_eq!(seqs, vec![1, 6], "只留发给 alice（默认租户）的");
            assert!(state.queue.iter().all(|queued| queued.user_id == "alice"));
            drop(state);
            assert!(
                !dir.join(EXTENSION_LOG).exists(),
                "别人的行读过也删掉，不留给下一个会话"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// 队列最多 500 条，满了丢最旧的；落盘后换个实例读回来；换了账号丢掉别人的。
        #[test]
        fn the_queue_is_bounded_and_persisted() {
            let root = temp_dir();
            let receipts = Arc::new(PushReceipts::default());
            receipts.set_root(root.clone());
            receipts.set_owner("alice", "0");
            {
                let mut state = receipts.lock();
                for seq in 0..520 {
                    let receipt = Receipt {
                        conversation_id: "c-1".into(),
                        message_id: String::new(),
                        seq,
                        collapse_id: "c:1".into(),
                        disposition: "loud".into(),
                        provider: "fcm".into(),
                        path: "app".into(),
                        received_at_ms: 0,
                    };
                    state.push("alice", "0", receipt);
                }
                assert_eq!(state.queue.len(), MAX_QUEUED);
                assert_eq!(state.queue[0].receipt.seq, 20, "最旧的先丢");
            }
            receipts.save();
            let reloaded = PushReceipts::default();
            reloaded.set_root(root.clone());
            assert_eq!(reloaded.lock().queue.len(), MAX_QUEUED);
            reloaded.set_owner("bob", "0");
            assert!(reloaded.lock().queue.is_empty());
            let _ = std::fs::remove_dir_all(&root);
        }

        const ACCEPTED: &str = r#"{"code":0,"data":{"accepted":1}}"#;
        const EXPIRED: &str = r#"{"code":401,"reason":"TOKEN_EXPIRED","message":""}"#;

        /// 本地的 Core API 替身：按顺序答 `replies`，记下每个请求带的 Authorization 头。
        async fn scripted_core(
            replies: Vec<(u16, &'static str)>,
        ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let header = |head: &str, wanted: &str| {
                    head.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case(wanted)
                            .then(|| value.trim().to_owned())
                    })
                };
                let mut seen = Vec::new();
                for (status, body) in replies {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buf = [0u8; 4096];
                    // 请求头与正文都读完再答：正文没读完就关连接，客户端收到的可能是重置而不是答复。
                    let (head_end, length) = loop {
                        let n = socket.read(&mut buf).await.unwrap();
                        assert!(n > 0, "the request ended early");
                        request.extend_from_slice(&buf[..n]);
                        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&request[..end]).to_string();
                            let length = header(&head, "content-length")
                                .and_then(|value| value.parse::<usize>().ok())
                                .unwrap_or(0);
                            break (end + 4, length);
                        }
                    };
                    while request.len() < head_end + length {
                        let n = socket.read(&mut buf).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        request.extend_from_slice(&buf[..n]);
                    }
                    let head = String::from_utf8_lossy(&request[..head_end]).to_string();
                    seen.push(header(&head, "authorization").unwrap_or_default());
                    let reply = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(reply.as_bytes()).await.unwrap();
                }
                seen
            });
            (url, server)
        }

        /// 续期换到一枚固定的新凭据。
        struct FixedRenewal(&'static str);

        #[async_trait::async_trait]
        impl crate::infrastructure::transport::http::CoreBearerRenewal for FixedRenewal {
            async fn renew(&self) -> Option<String> {
                Some(self.0.to_owned())
            }
        }

        /// alice 排着一条回执、上报通道接在 `url` 上（共享上下文 `context`）的队列。
        fn queued_for_flush(
            url: &str,
            context: &Arc<crate::infrastructure::transport::http::HttpRequestContext>,
            generation: &Arc<AtomicU64>,
        ) -> PushReceipts {
            let receipts = PushReceipts::default();
            receipts.set_owner("alice", "0");
            receipts.lock().push(
                "alice",
                "0",
                Receipt {
                    conversation_id: "c-1".into(),
                    message_id: "m-9".into(),
                    seq: 1,
                    collapse_id: "c:1".into(),
                    disposition: "loud".into(),
                    provider: "apns".into(),
                    path: "extension".into(),
                    received_at_ms: 1,
                },
            );
            receipts.attach(ReceiptSender {
                device_id: "d-1".into(),
                http: HttpClient::with_context(url, context.clone()).with_core_bearer(),
                user_id: "alice".into(),
                tenant_id: "0".into(),
                epoch: 1,
                generation: generation.clone(),
            });
            receipts
        }

        /// 上报用会话当前的凭据：装上通道之后刷新过的那枚，不是装上时的那枚。
        #[tokio::test]
        async fn the_flush_uses_the_current_credential() {
            let context =
                Arc::new(crate::infrastructure::transport::http::HttpRequestContext::new());
            context.set_auth_context("attach-token".into(), None).await;
            let generation = Arc::new(AtomicU64::new(1));
            let (url, server) = scripted_core(vec![(200, ACCEPTED)]).await;
            let receipts = queued_for_flush(&url, &context, &generation);
            context
                .set_auth_context("refreshed-token".into(), None)
                .await;
            receipts.flush().await;
            assert_eq!(
                server.await.unwrap(),
                vec!["Bearer refreshed-token".to_owned()]
            );
            assert!(receipts.lock().queue.is_empty());
        }

        /// 凭据过期：和别的 Core 请求一样续期一次，用新的再发，新的留在共享上下文里。
        #[tokio::test]
        async fn an_expired_credential_is_renewed_once() {
            let context =
                Arc::new(crate::infrastructure::transport::http::HttpRequestContext::new());
            context.set_auth_context("expired-token".into(), None).await;
            context
                .set_core_bearer_renewal(Some(Arc::new(FixedRenewal("fresh-token"))))
                .await;
            let generation = Arc::new(AtomicU64::new(1));
            let (url, server) = scripted_core(vec![(401, EXPIRED), (200, ACCEPTED)]).await;
            let receipts = queued_for_flush(&url, &context, &generation);
            receipts.flush().await;
            assert_eq!(
                server.await.unwrap(),
                vec![
                    "Bearer expired-token".to_owned(),
                    "Bearer fresh-token".to_owned()
                ]
            );
            assert!(receipts.lock().queue.is_empty());
            let client = HttpClient::with_context("http://127.0.0.1:1", context.clone());
            assert_eq!(
                client.current_core_bearer().await.as_deref(),
                Some("fresh-token")
            );
        }

        /// 续期也没换到新的：按 401 留着下次再报，只发了一次。
        #[tokio::test]
        async fn a_failed_renewal_keeps_the_batch() {
            let context =
                Arc::new(crate::infrastructure::transport::http::HttpRequestContext::new());
            context.set_auth_context("expired-token".into(), None).await;
            let generation = Arc::new(AtomicU64::new(1));
            let (url, server) = scripted_core(vec![(401, EXPIRED)]).await;
            let receipts = queued_for_flush(&url, &context, &generation);
            receipts.flush().await;
            assert_eq!(server.await.unwrap().len(), 1);
            assert_eq!(receipts.lock().queue.len(), 1);
        }

        /// 会话换代了：一条也不发，排着的留给同一账号的下一个会话。
        #[tokio::test]
        async fn another_generation_sends_nothing() {
            let context =
                Arc::new(crate::infrastructure::transport::http::HttpRequestContext::new());
            context
                .set_auth_context("next-account-token".into(), None)
                .await;
            let generation = Arc::new(AtomicU64::new(2));
            let receipts = queued_for_flush("http://127.0.0.1:1", &context, &generation);
            receipts.flush().await;
            assert_eq!(receipts.lock().queue.len(), 1);
        }

        /// 4xx 丢弃；401、408、429、5xx 与网络错误留着。
        #[test]
        fn errors_are_classified_by_status() {
            let status = |code: u16| {
                crate::infrastructure::transport::http::http_error_from_response_status(code, "")
            };
            assert_eq!(classify_error(&status(400)), Sent::Rejected);
            assert_eq!(classify_error(&status(403)), Sent::Rejected);
            assert_eq!(classify_error(&status(401)), Sent::Retry);
            assert_eq!(classify_error(&status(429)), Sent::Retry);
            assert_eq!(classify_error(&status(503)), Sent::Retry);
            assert_eq!(
                classify_error(&FlareError::general_error("connection refused")),
                Sent::Retry
            );
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod stub {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use crate::infrastructure::transport::http::HttpClient;

    /// 浏览器里没有系统推送，也就没有回执。
    #[derive(Clone)]
    pub(crate) struct ReceiptSender {
        pub device_id: String,
        pub http: HttpClient,
        pub user_id: String,
        pub tenant_id: String,
        pub epoch: u64,
        pub generation: Arc<AtomicU64>,
    }

    /// 浏览器里的回执：什么都不做。
    #[derive(Default)]
    pub(crate) struct PushReceipts;

    impl PushReceipts {
        pub(crate) fn set_root(&self, _root: PathBuf) {}
        pub(crate) fn set_owner(&self, _user_id: &str, _tenant_id: &str) {}
        pub(crate) fn attach(&self, _sender: ReceiptSender) {}
        pub(crate) fn detach(&self) {}
        pub(crate) fn set_app_version(&self, _app_version: Option<String>) {}
        pub(crate) fn record_app(
            self: &Arc<Self>,
            _provider: &str,
            _payload: &BTreeMap<String, String>,
        ) {
        }
        pub(crate) fn ingest(&self, _directory: &Path) -> usize {
            0
        }
        pub(crate) async fn flush(&self) {}
    }
}
