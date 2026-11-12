//! 提示台账：每个合并键已经提示到哪个 seq、清除到哪个 seq。
//!
//! 同一条消息可能从两条路到达用户：链路在线时引擎确认了它（谁确认谁负责提示），确认却在路上丢了，
//! 服务端照样发推送；推送本身也会重复、乱序。两条路用同一个合并键（`c:` + 会话短哈希，一个会话一条
//! 通知），所以按合并键记下已提示的最大 seq，推送到达时比一下就知道该不该再响。
//!
//! 只在内存里、有上限：进程重启后系统通知栏里的通知照样由合并键覆盖，最坏是多响一次。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use sha2::{Digest, Sha256};

/// 台账最多记这么多个合并键；再多就丢最久没碰过的那个。
pub(crate) const ALERT_LEDGER_CAPACITY: usize = 512;

/// 一条推送不再展示的原因（诊断用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlertSkip {
    /// 同一合并键已经在相同或更大的 seq 上提示过。
    AlreadyAlerted,
    /// 已经有更新的通知，或这一段已被清除过。
    Stale,
}

#[derive(Debug, Clone, Copy, Default)]
struct Mark {
    /// 已提示（或由本进程确认、承诺提示）的最大 seq。
    alerted: u64,
    /// 已清除到的 seq。
    cleared: u64,
    /// 最近一次被碰到的时刻（台账内的逻辑时钟），满了按它淘汰。
    used: u64,
}

#[derive(Default)]
struct Ledger {
    /// 链路记账从会话算合并键，合并键里带租户。
    tenant: Option<String>,
    marks: HashMap<String, Mark>,
    clock: u64,
}

impl Ledger {
    /// 取（必要时新建）一个合并键的记录并记下这次使用；满了先淘汰最久没碰过的。
    fn touch(&mut self, collapse_id: &str) -> &mut Mark {
        self.clock += 1;
        let clock = self.clock;
        if !self.marks.contains_key(collapse_id) && self.marks.len() >= ALERT_LEDGER_CAPACITY {
            let oldest = self
                .marks
                .iter()
                .min_by_key(|(_, mark)| mark.used)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.marks.remove(&oldest);
            }
        }
        let mark = self.marks.entry(collapse_id.to_owned()).or_default();
        mark.used = clock;
        mark
    }
}

/// 一个会话（一个登录账号）的提示台账。链路的分发器往里记「本进程确认了、由我提示」，推送入口往里
/// 记「推送展示了、清除了」，两边都按它判断还该不该提示。
#[derive(Default)]
pub(crate) struct AlertLedger {
    state: Mutex<Ledger>,
}

impl AlertLedger {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Ledger> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 定下台账属于哪个租户：链路确认的消息要靠它算出合并键。租户变了（换了账号），旧记录全部作废。
    pub fn scope(&self, tenant_id: &str) {
        let mut ledger = self.lock();
        if ledger.tenant.as_deref() != Some(tenant_id) {
            ledger.tenant = Some(tenant_id.to_owned());
            ledger.marks.clear();
        }
    }

    /// 会话的合并键（按台账定下的租户）。还没定下租户时为 `None`。
    pub fn collapse_id(&self, conversation_id: &str) -> Option<String> {
        let ledger = self.lock();
        let tenant = ledger.tenant.as_deref()?;
        Some(conversation_collapse_id(tenant, conversation_id))
    }

    /// 链路上实时到达、由本进程确认了的消息（会话、seq）。确认就是承诺由本进程提示，所以同一条的
    /// 推送再到达时不该再响。还没定下租户时算不出合并键，不记。
    pub fn note_confirmed(&self, delivered: &[(String, u64)]) {
        let mut ledger = self.lock();
        let Some(tenant) = ledger.tenant.clone() else {
            return;
        };
        for (conversation_id, seq) in delivered {
            if conversation_id.is_empty() || *seq == 0 {
                continue;
            }
            let mark = ledger.touch(&conversation_collapse_id(&tenant, conversation_id));
            mark.alerted = mark.alerted.max(*seq);
        }
    }

    /// 推送要在 `collapse_id` 上展示 `seq`。可以展示时记下并返回 `Ok`；同一处已经清除过这一段时是
    /// [`AlertSkip::Stale`]。
    ///
    /// `replace` 为假（新通知、静默更新）：已在相同或更大的 seq 上提示过就是
    /// [`AlertSkip::AlreadyAlerted`]。为真（撤回替换）：替换的正是已经展示的那条，相同的 seq 照常
    /// 放行，只有更新的通知已经展示时才是 [`AlertSkip::Stale`]。
    pub fn claim_alert(&self, collapse_id: &str, seq: u64, replace: bool) -> Result<(), AlertSkip> {
        let mut ledger = self.lock();
        let mark = ledger.touch(collapse_id);
        if mark.cleared >= seq {
            return Err(AlertSkip::Stale);
        }
        if replace {
            if mark.alerted > seq {
                return Err(AlertSkip::Stale);
            }
        } else if mark.alerted >= seq {
            return Err(AlertSkip::AlreadyAlerted);
        }
        mark.alerted = mark.alerted.max(seq);
        Ok(())
    }

    /// 提示过、还没清除的合并键与它们提示到的 seq。应用回到前台时据此撤掉已读会话的通知。
    pub fn uncleared(&self) -> Vec<(String, u64)> {
        self.lock()
            .marks
            .iter()
            .filter(|(_, mark)| mark.alerted > mark.cleared)
            .map(|(collapse_id, mark)| (collapse_id.clone(), mark.alerted))
            .collect()
    }

    /// 清除 `collapse_id` 上截至 `seq` 的通知。展示着的已经是更新的一条（`seq` 之后的消息）时不能
    /// 清，返回 [`AlertSkip::Stale`]。
    pub fn claim_clear(&self, collapse_id: &str, seq: u64) -> Result<(), AlertSkip> {
        let mut ledger = self.lock();
        let mark = ledger.touch(collapse_id);
        if mark.alerted > seq {
            return Err(AlertSkip::Stale);
        }
        mark.cleared = mark.cleared.max(seq);
        Ok(())
    }
}

/// 会话的合并键：`c:` 加 `SHA-256(租户 \0 会话)` 前 16 字节的小写十六进制。与服务端推送任务里的
/// 合并键逐字节相同，链路与推送才会落在同一条通知上。
pub(crate) fn conversation_collapse_id(tenant_id: &str, conversation_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(conversation_id.as_bytes());
    let digest = hasher.finalize();
    format!("c:{}", hex::encode(&digest[..16]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 与服务端 `conversation_collapse_key` 同一算法：租户与会话之间的 0 字节让 ("a","bc") 与
    /// ("ab","c") 不撞。
    #[test]
    fn the_collapse_id_matches_the_servers() {
        assert_eq!(
            conversation_collapse_id("tenant-a", "conv-1"),
            "c:f89728d36a2ef31c5683db37558cda28"
        );
        assert_eq!(
            conversation_collapse_id("0", "c1"),
            "c:e9b5527ed1b01e066e19de950a2f9dbe"
        );
        assert_ne!(
            conversation_collapse_id("a", "bc"),
            conversation_collapse_id("ab", "c")
        );
    }

    /// 同一合并键只在更大的 seq 上再提示一次；重复、乱序到达的不再响。
    #[test]
    fn an_alert_is_claimed_once_per_seq() {
        let ledger = AlertLedger::new();
        assert_eq!(ledger.claim_alert("c:1", 5, false), Ok(()));
        assert_eq!(
            ledger.claim_alert("c:1", 5, false),
            Err(AlertSkip::AlreadyAlerted)
        );
        assert_eq!(
            ledger.claim_alert("c:1", 4, false),
            Err(AlertSkip::AlreadyAlerted)
        );
        assert_eq!(ledger.claim_alert("c:1", 6, false), Ok(()));
        assert_eq!(
            ledger.claim_alert("c:2", 1, false),
            Ok(()),
            "别的会话不受影响"
        );
    }

    /// 撤回替换换的就是展示着的那条：相同 seq 放行，更新的通知已展示时不换。
    #[test]
    fn a_recall_replaces_the_alert_it_names() {
        let ledger = AlertLedger::new();
        ledger.claim_alert("c:1", 7, false).unwrap();
        assert_eq!(ledger.claim_alert("c:1", 7, true), Ok(()));
        assert_eq!(ledger.claim_alert("c:1", 6, true), Err(AlertSkip::Stale));
    }

    /// 清除不能清掉更新的通知；清过的一段再来提示也不再响。
    #[test]
    fn a_clear_never_hides_a_newer_alert() {
        let ledger = AlertLedger::new();
        ledger.claim_alert("c:1", 9, false).unwrap();
        assert_eq!(ledger.claim_clear("c:1", 8), Err(AlertSkip::Stale));
        assert_eq!(ledger.claim_clear("c:1", 9), Ok(()));
        assert_eq!(ledger.claim_alert("c:2", 3, false), Ok(()));
        assert_eq!(ledger.claim_clear("c:2", 5), Ok(()));
        assert_eq!(ledger.claim_alert("c:2", 4, false), Err(AlertSkip::Stale));
        assert_eq!(ledger.claim_alert("c:2", 6, false), Ok(()));
    }

    /// 链路确认的消息按会话算出合并键记账：之后同一条的推送不再响。没定租户时不记；换了租户清空。
    #[test]
    fn link_confirmations_count_as_alerts() {
        let ledger = AlertLedger::new();
        ledger.note_confirmed(&[("conv-1".to_owned(), 4)]);
        let collapse = conversation_collapse_id("tenant-a", "conv-1");
        assert_eq!(
            ledger.claim_alert(&collapse, 4, false),
            Ok(()),
            "租户未定，没有记账"
        );

        let ledger = AlertLedger::new();
        ledger.scope("tenant-a");
        ledger.note_confirmed(&[("conv-1".to_owned(), 4), ("".to_owned(), 9)]);
        assert_eq!(
            ledger.claim_alert(&collapse, 4, false),
            Err(AlertSkip::AlreadyAlerted)
        );
        assert_eq!(ledger.claim_alert(&collapse, 5, false), Ok(()));

        ledger.scope("tenant-a");
        assert_eq!(
            ledger.claim_alert(&collapse, 5, false),
            Err(AlertSkip::AlreadyAlerted),
            "同一租户重复定下不清空"
        );
        ledger.scope("tenant-b");
        assert_eq!(
            ledger.claim_alert(&collapse, 5, false),
            Ok(()),
            "换了租户清空"
        );
    }

    /// 还没清除的只剩提示过而清除没盖过的那些。
    #[test]
    fn uncleared_lists_alerts_not_yet_cleared() {
        let ledger = AlertLedger::new();
        ledger.claim_alert("c:1", 4, false).unwrap();
        ledger.claim_alert("c:2", 6, false).unwrap();
        ledger.claim_clear("c:2", 6).unwrap();
        assert_eq!(ledger.uncleared(), vec![("c:1".to_owned(), 4)]);
    }

    /// 合并键按定下的租户算；没定租户时算不出来。
    #[test]
    fn the_collapse_id_follows_the_scoped_tenant() {
        let ledger = AlertLedger::new();
        assert_eq!(ledger.collapse_id("conv-1"), None);
        ledger.scope("tenant-a");
        assert_eq!(
            ledger.collapse_id("conv-1").as_deref(),
            Some("c:f89728d36a2ef31c5683db37558cda28")
        );
    }

    /// 有上限：满了淘汰最久没碰过的，最近碰过的留着。
    #[test]
    fn the_ledger_is_bounded() {
        let ledger = AlertLedger::new();
        for i in 0..ALERT_LEDGER_CAPACITY {
            ledger.claim_alert(&format!("c:{i}"), 1, false).unwrap();
        }
        // 碰一下最早的那个，它就不再是最久没用的。
        assert_eq!(
            ledger.claim_alert("c:0", 1, false),
            Err(AlertSkip::AlreadyAlerted)
        );
        ledger.claim_alert("c:new", 1, false).unwrap();
        assert_eq!(ledger.lock().marks.len(), ALERT_LEDGER_CAPACITY);
        assert_eq!(
            ledger.claim_alert("c:0", 1, false),
            Err(AlertSkip::AlreadyAlerted)
        );
        assert_eq!(ledger.claim_alert("c:1", 1, false), Ok(()), "c:1 已被淘汰");
    }
}
