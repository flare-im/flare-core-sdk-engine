//! 通知快照：给 iOS 通知扩展读的一份小文件（`<目录>/snapshot.json`）。
//!
//! 通知扩展是另一个进程，内存上限很低，不能打开主库，也不跑同步。它只读这份快照：用本地的会话标题、
//! 联系人显示名与预览设置，把服务端兜底的文案重新渲染一遍。快照由主应用的引擎写：
//!
//! - 会话列表、会话设置（免打扰、仅 @我、标题）、联系人显示名、用户级通知设置变了之后去抖 2 秒写一次，
//!   会话开始（准备好、建连）时写一次；内容与上次写的相同就不写；
//! - 写临时文件再改名，扩展读不到写了一半的；
//! - 登出时删掉，免得下一个账号的推送用上一个账号的名字；
//! - 不超过 256 KiB：先丢超出预算的名字，再丢最久没动的会话。
//!
//! 文件里有会话标题与联系人名字，没有消息内容、令牌与凭据。只在原生端编译：浏览器里没有通知扩展。

use std::cmp::Reverse;
use std::collections::HashSet;
use std::path::Path;

use flare_notify_rules::text::{Phrase, phrase};
use serde::Serialize;

use super::*;
use crate::shared::util::atomic_file::{remove_if_present, write_atomic};

/// 快照的文件名（在宿主给的目录下）。
const SNAPSHOT_FILE: &str = "snapshot.json";
/// 变化之后等多久再写：一批同步带来的一串变化只写一次。
const SNAPSHOT_DEBOUNCE: Duration = Duration::from_secs(2);
/// 文件大小上限：扩展的内存预算很紧，读、解析都要在几十毫秒内完成。
const SNAPSHOT_MAX_BYTES: usize = 256 * 1024;
/// 最多写多少个会话（按最近活动排）。
const SNAPSHOT_CONVERSATIONS: usize = 500;
/// 读会话列表与名字的时限。
const SNAPSHOT_READ_CAP: Duration = Duration::from_secs(5);
/// 快照格式的版本。
const SNAPSHOT_VERSION: u32 = 1;

/// NotifyCenter 手上与快照有关的状态。
pub(super) struct SnapshotSlot {
    /// 写进快照的语言标签：宿主配置的应用语言；没配时写 `zh`（文案此时按中文渲染）。
    locale: String,
    state: Mutex<SnapshotState>,
    /// 写文件与删文件排成一队：登出时删除一定落在最后一次写之后。会话结束后删除在后台进行，
    /// 那时引擎可能已经释放，所以锁单独共享出去。
    io: Arc<tokio::sync::Mutex<()>>,
    /// 盯着联系人显示名变化的任务。
    watcher: Mutex<Option<BackgroundTask>>,
}

#[derive(Default)]
struct SnapshotState {
    /// 这一个登录会话的用户与租户；还没报过时不写。
    identity: Option<(String, String)>,
    /// 已经登记了一次写入，还没开始。
    scheduled: bool,
    /// 上一次写出的内容（不含时间戳）的摘要：没变就不再写。
    written: Option<u64>,
}

impl SnapshotSlot {
    pub(super) fn new(locale: Option<&str>) -> Self {
        let locale = locale
            .map(str::trim)
            .filter(|locale| !locale.is_empty())
            .unwrap_or("zh")
            .to_owned();
        Self {
            locale,
            state: Mutex::new(SnapshotState::default()),
            io: Arc::new(tokio::sync::Mutex::new(())),
            watcher: Mutex::new(None),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, SnapshotState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for SnapshotSlot {
    fn drop(&mut self) {
        if let Some(watcher) = self
            .watcher
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            watcher.abort();
        }
    }
}

/// 快照里的一个会话。键短，500 个会话也只占几十 KiB。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
struct SnapshotConversation {
    /// 本地显示的标题；本地还没有名字时为空。
    t: String,
    /// 免打扰。
    m: bool,
    /// 只提示 @我（含没开「全部消息通知」的大群）。
    o: bool,
    /// 多人会话（群、频道、广播）。
    g: bool,
}

/// 扩展自己出文案时用的固定说法，按快照的语言渲染。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotStrings {
    generic_title: &'static str,
    generic_body: &'static str,
    sent_message: &'static str,
    mention_marker: &'static str,
    recalled: &'static str,
}

impl SnapshotStrings {
    fn of(lang: Lang) -> Self {
        Self {
            generic_title: phrase(lang, Phrase::GenericTitle),
            generic_body: phrase(lang, Phrase::GenericBody),
            sent_message: phrase(lang, Phrase::SentMessage),
            mention_marker: phrase(lang, Phrase::MentionMarker),
            recalled: phrase(lang, Phrase::Recalled),
        }
    }
}

/// 一份快照的内容，会话按最近活动排（最新的在前），名字按出现的先后排（最近的会话里的人在前）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SnapshotContent {
    uid: String,
    tenant: String,
    locale: String,
    preview: &'static str,
    strings: SnapshotStrings,
    conversations: Vec<(String, SnapshotConversation)>,
    names: Vec<(String, String)>,
}

impl SnapshotContent {
    /// 不含时间戳的摘要：内容没变就不再写。
    fn digest(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// 编码成 JSON，保留最新的 `conversations` 个会话与最前面的 `names` 个名字。
    fn encode(&self, conversations: usize, names: usize, at_ms: i64) -> Vec<u8> {
        let conversation_map: serde_json::Map<String, serde_json::Value> = self.conversations
            [..conversations]
            .iter()
            .map(|(id, conversation)| {
                (
                    id.clone(),
                    serde_json::to_value(conversation).unwrap_or_default(),
                )
            })
            .collect();
        let name_map: serde_json::Map<String, serde_json::Value> = self.names[..names]
            .iter()
            .map(|(user_id, name)| (user_id.clone(), serde_json::Value::from(name.as_str())))
            .collect();
        let value = serde_json::json!({
            "v": SNAPSHOT_VERSION,
            "uid": self.uid,
            "tenant": self.tenant,
            "locale": self.locale,
            "preview": self.preview,
            "strings": self.strings,
            "conversations": conversation_map,
            "names": name_map,
            "at": at_ms,
        });
        serde_json::to_vec(&value).unwrap_or_default()
    }

    /// 编码并压到 `max_bytes` 以内：先从后往前丢名字，名字丢完了再丢最久没动的会话。
    fn render(&self, max_bytes: usize, at_ms: i64) -> Vec<u8> {
        let conversation_sizes: Vec<usize> = self
            .conversations
            .iter()
            .map(|(id, conversation)| entry_size(id, conversation))
            .collect();
        let name_sizes: Vec<usize> = self
            .names
            .iter()
            .map(|(user_id, name)| entry_size(user_id, name))
            .collect();
        let mut conversations = self.conversations.len();
        let mut names = self.names.len();
        loop {
            let bytes = self.encode(conversations, names, at_ms);
            if bytes.len() <= max_bytes || (conversations == 0 && names == 0) {
                return bytes;
            }
            let excess = bytes.len() - max_bytes;
            if names > 0 {
                names = keep_within(&name_sizes[..names], excess);
            } else {
                conversations = keep_within(&conversation_sizes[..conversations], excess);
            }
        }
    }
}

/// 一个条目在 JSON 对象里大约占多少字节（键、冒号、值、逗号）。
fn entry_size(key: &str, value: &impl Serialize) -> usize {
    let key = serde_json::to_string(key).map_or(key.len() + 2, |key| key.len());
    let value = serde_json::to_string(value).map_or(0, |value| value.len());
    key + value + 2
}

/// 从末尾丢条目，丢掉的合计至少 `excess` 字节（至少丢一个），返回剩下的个数。
fn keep_within(sizes: &[usize], excess: usize) -> usize {
    let mut kept = sizes.len();
    let mut dropped = 0;
    while kept > 0 {
        kept -= 1;
        dropped += sizes[kept];
        if dropped >= excess {
            break;
        }
    }
    kept
}

/// 会话最近一次活动：最后一条消息的时间，没有时用更新时间。
fn recency(conversation: &Conversation) -> u64 {
    conversation
        .last_message_at
        .unwrap_or(0)
        .max(conversation.updated_at)
}

fn preview_name(preview: NotificationPreview) -> &'static str {
    match preview {
        NotificationPreview::Full => "full",
        NotificationPreview::SenderOnly => "sender_only",
        NotificationPreview::Hidden => "hidden",
    }
}

/// 可能在这个会话里发消息的人：最后一条的发送者、单聊的对方、成员预览与已同步的成员。
fn senders_of<'a>(conversation: &'a Conversation, user_id: &str) -> Vec<&'a str> {
    let mut senders = Vec::new();
    let last = conversation
        .last_message
        .as_ref()
        .map(|last| last.sender_id.as_str())
        .or(conversation.last_sender_id.as_deref());
    senders.extend(last);
    if conversation.conversation_type == ConversationType::Single {
        senders.push(conversation.channel_id.as_str());
    }
    senders.extend(
        conversation
            .member_preview
            .iter()
            .chain(conversation.participants.iter())
            .map(|member| member.user_id.as_str()),
    );
    senders
        .into_iter()
        .map(str::trim)
        .filter(|sender| !sender.is_empty() && *sender != user_id)
        .collect()
}

impl NotifyCenter {
    /// 盯着联系人显示名的变化（会话与设置的变化由各自的路径报进来）。
    pub(super) fn start_snapshot_watcher(&self) {
        let names = self.bus.subscribe_name_changes();
        let center = self.me.clone();
        let task = spawn_background_task(async move {
            loop {
                let _ = names.recv().await;
                let Some(center) = center.upgrade() else {
                    break;
                };
                if center.ended() {
                    break;
                }
                center.snapshot_changed();
            }
        });
        *self
            .snapshot
            .watcher
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(task);
    }

    /// 这一个登录会话属于谁（准备好、建连开始时各报一次）：随即写一份快照。
    pub(crate) fn attach_session(&self, user_id: &str, tenant_id: &str) {
        if user_id.trim().is_empty() {
            return;
        }
        self.snapshot.lock_state().identity = Some((user_id.to_owned(), tenant_id.to_owned()));
        self.write_snapshot_after(Duration::ZERO);
    }

    /// 宿主换了与通知扩展共用的目录：在新目录里写一份（旧目录里的不动）。
    pub(crate) fn snapshot_directory_changed(&self) {
        self.snapshot.lock_state().written = None;
        self.write_snapshot_after(Duration::ZERO);
    }

    /// 快照里的东西可能变了：去抖之后写。
    pub(crate) fn snapshot_changed(&self) {
        self.write_snapshot_after(SNAPSHOT_DEBOUNCE);
    }

    /// 登记过、还没写出的那次现在就写（后台执行时间快用完时调用）。
    pub(crate) async fn flush_snapshot(&self) {
        if self.snapshot.lock_state().scheduled {
            self.write_snapshot().await;
        }
    }

    fn write_snapshot_after(&self, after: Duration) {
        {
            let mut state = self.snapshot.lock_state();
            if state.identity.is_none() || state.scheduled || self.ended() {
                return;
            }
            state.scheduled = true;
        }
        let center = self.me.clone();
        spawn_background(async move {
            if !after.is_zero() {
                delay(after).await;
            }
            if let Some(center) = center.upgrade() {
                center.write_snapshot().await;
            }
        });
    }

    async fn write_snapshot(&self) {
        let io = self.snapshot.io.clone();
        let _io = io.lock().await;
        let identity = {
            let mut state = self.snapshot.lock_state();
            if !state.scheduled {
                // 别的路径刚写过（例如后台刷新收尾时）。
                return;
            }
            state.scheduled = false;
            state.identity.clone()
        };
        let Some((user_id, tenant_id)) = identity else {
            return;
        };
        let Some(directory) = self.host.load().snapshot_directory() else {
            return;
        };
        if self.ended() {
            return;
        }
        let conversations = match timeout(SNAPSHOT_READ_CAP, self.stores.conversations.list()).await
        {
            Ok(Ok(conversations)) => conversations,
            Ok(Err(error)) => {
                tracing::debug!(%error, "reading conversations for the notification snapshot failed");
                return;
            }
            Err(_) => {
                tracing::debug!("reading conversations for the notification snapshot timed out");
                return;
            }
        };
        let content = self
            .snapshot_content(&user_id, &tenant_id, conversations)
            .await;
        let digest = content.digest();
        if self.snapshot.lock_state().written == Some(digest) {
            return;
        }
        let bytes = content.render(SNAPSHOT_MAX_BYTES, now_millis() as i64);
        // 读库期间会话结束了：不再写，等着删文件的那一方马上接手。
        if self.ended() {
            return;
        }
        match write_atomic(&directory.join(SNAPSHOT_FILE), &bytes) {
            Ok(()) => self.snapshot.lock_state().written = Some(digest),
            Err(error) => {
                tracing::debug!(%error, "writing the notification snapshot failed");
            }
        }
    }

    /// 读会话与名字，出一份快照的内容。
    async fn snapshot_content(
        &self,
        user_id: &str,
        tenant_id: &str,
        mut conversations: Vec<Conversation>,
    ) -> SnapshotContent {
        conversations.sort_by_key(|conversation| Reverse(recency(conversation)));
        conversations.truncate(SNAPSHOT_CONVERSATIONS);
        let mut request = NameRequest::default();
        for conversation in &conversations {
            request.conversation(conversation, user_id);
        }
        let profiles = self.stores.user_profiles_or_memory();
        let book = match timeout(
            SNAPSHOT_READ_CAP,
            request.load(profiles.as_ref(), Some(self.stores.conversations.as_ref())),
        )
        .await
        {
            Ok(book) => book,
            Err(_) => {
                tracing::debug!("reading names for the notification snapshot timed out");
                NameBook::default()
            }
        };
        let mut entries = Vec::with_capacity(conversations.len());
        let mut names = Vec::new();
        let mut named: HashSet<String> = HashSet::new();
        for conversation in &conversations {
            let mut projected = conversation.clone();
            book.project_conversation(&mut projected, user_id);
            let prefs = conversation_prefs(Some(conversation));
            entries.push((
                conversation.conversation_id.clone(),
                SnapshotConversation {
                    t: display_text(&projected.title, self.lang).unwrap_or_default(),
                    m: prefs.muted,
                    o: prefs.mention_only,
                    g: matches!(
                        conversation_kind(conversation.conversation_type),
                        ConversationKind::Group
                    ),
                },
            ));
            for sender in senders_of(conversation, user_id) {
                if named.contains(sender) {
                    continue;
                }
                // 名字表不分会话：备注优先，再是资料名，最后是消息带的发送者名字；群昵称不算。
                let snapshot = if conversation.last_sender_id.as_deref() == Some(sender) {
                    conversation.last_sender_nickname.as_str()
                } else {
                    ""
                };
                let candidates = book.candidates("", sender, "", snapshot);
                if let Some(name) = resolve_person_name(sender, &candidates, None).text() {
                    named.insert(sender.to_owned());
                    names.push((sender.to_owned(), name.to_owned()));
                }
            }
        }
        let preview = self
            .cached_settings()
            .map(|settings| settings.preview)
            .unwrap_or_default();
        SnapshotContent {
            uid: user_id.to_owned(),
            tenant: tenant_id.to_owned(),
            locale: self.snapshot.locale.clone(),
            preview: preview_name(preview),
            strings: SnapshotStrings::of(self.lang),
            conversations: entries,
            names,
        }
    }

    /// 会话结束：之后不再写，宿主给的目录里的快照删掉（排在进行中的那次写之后）。
    pub(super) fn delete_snapshot(&self) {
        {
            let mut state = self.snapshot.lock_state();
            state.identity = None;
            state.scheduled = false;
            state.written = None;
        }
        let Some(directory) = self.host.load().snapshot_directory() else {
            return;
        };
        let io = self.snapshot.io.clone();
        spawn_background(async move {
            let _io = io.lock().await;
            delete_snapshot_file(&directory);
        });
    }
}

/// 删掉 `directory` 下的快照。
fn delete_snapshot_file(directory: &Path) {
    if let Err(error) = remove_if_present(&directory.join(SNAPSHOT_FILE)) {
        tracing::debug!(%error, "deleting the notification snapshot failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(conversations: usize, names: usize) -> SnapshotContent {
        SnapshotContent {
            uid: "u-1".into(),
            tenant: "t-1".into(),
            locale: "zh-CN".into(),
            preview: "full",
            strings: SnapshotStrings::of(Lang::Zh),
            conversations: (0..conversations)
                .map(|index| {
                    (
                        format!("c-{index:04}"),
                        SnapshotConversation {
                            t: format!("会话 {index}"),
                            m: index % 2 == 0,
                            o: false,
                            g: true,
                        },
                    )
                })
                .collect(),
            names: (0..names)
                .map(|index| (format!("u-{index:05}"), format!("联系人 {index}")))
                .collect(),
        }
    }

    /// 形状与 iface 一致：版本、身份、语言、预览、五句固定文案、会话、名字、时间。
    #[test]
    fn the_snapshot_has_the_agreed_shape() {
        let bytes = content(1, 1).render(SNAPSHOT_MAX_BYTES, 1_731_400_000_000);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["v"], 1);
        assert_eq!(value["uid"], "u-1");
        assert_eq!(value["tenant"], "t-1");
        assert_eq!(value["locale"], "zh-CN");
        assert_eq!(value["preview"], "full");
        assert_eq!(value["at"], 1_731_400_000_000_i64);
        assert_eq!(value["strings"]["genericTitle"], "新消息");
        assert_eq!(value["strings"]["genericBody"], "你收到一条新消息");
        assert_eq!(value["strings"]["sentMessage"], "发来一条消息");
        assert_eq!(value["strings"]["mentionMarker"], "[有人@你]");
        assert_eq!(value["strings"]["recalled"], "撤回了一条消息");
        assert_eq!(
            value["conversations"]["c-0000"],
            serde_json::json!({"t": "会话 0", "m": true, "o": false, "g": true})
        );
        assert_eq!(value["names"]["u-00000"], "联系人 0");
    }

    /// 超出预算先丢名字（从后往前），名字丢完了才丢最久没动的会话。
    #[test]
    fn the_budget_drops_names_first_then_the_oldest_conversations() {
        let snapshot = content(400, 20_000);
        let bytes = snapshot.render(SNAPSHOT_MAX_BYTES, 0);
        assert!(bytes.len() <= SNAPSHOT_MAX_BYTES);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["conversations"].as_object().unwrap().len(), 400);
        let names = value["names"].as_object().unwrap();
        assert!(!names.is_empty() && names.len() < 20_000);
        assert!(names.contains_key("u-00000"), "最前面的名字留下");

        let snapshot = content(500, 0);
        let bytes = snapshot.render(4_096, 0);
        assert!(bytes.len() <= 4_096);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let conversations = value["conversations"].as_object().unwrap();
        assert!(!conversations.is_empty() && conversations.len() < 500);
        assert!(conversations.contains_key("c-0000"), "最新的会话留下");
        assert!(!conversations.contains_key("c-0499"), "最久没动的先丢");
    }

    /// 摘要不含时间戳：内容不变就不重写。
    #[test]
    fn the_digest_ignores_the_timestamp() {
        assert_eq!(content(3, 3).digest(), content(3, 3).digest());
        assert_ne!(content(3, 3).digest(), content(3, 2).digest());
    }

    /// 预览设置写成 snake_case。
    #[test]
    fn preview_modes_use_snake_case() {
        assert_eq!(preview_name(NotificationPreview::Full), "full");
        assert_eq!(preview_name(NotificationPreview::SenderOnly), "sender_only");
        assert_eq!(preview_name(NotificationPreview::Hidden), "hidden");
    }

    /// 发送者：最后一条的发送者、单聊对方、成员；自己与空 ID 不算。
    #[test]
    fn senders_come_from_the_row_and_skip_the_user() {
        let conversation = Conversation {
            conversation_id: "c1".into(),
            conversation_type: ConversationType::Single,
            channel_id: "bob".into(),
            last_sender_id: Some("me".into()),
            ..Conversation::default()
        };
        assert_eq!(senders_of(&conversation, "me"), vec!["bob"]);
    }

    /// 没配语言时写 `zh`。
    #[test]
    fn the_locale_defaults_to_chinese() {
        assert_eq!(SnapshotSlot::new(None).locale, "zh");
        assert_eq!(SnapshotSlot::new(Some(" en-US ")).locale, "en-US");
    }
}
