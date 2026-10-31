//! 用户资料（显示名称、头像）— 用于消息/会话展示
//!
//! 可由 SyncTask 同步或本地缓存填充。名字由 [`crate::domain::resolve_person_name`] 决定，
//! 没有名字时投影为结构化的「无名」，不把 user_id 当名字。

use serde::{Deserialize, Serialize};

/// 用户资料：显示名称 + 头像，供消息发送者/会话最后一条发送者展示
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserProfile {
    pub user_id: String,
    /// 展示用名称（昵称或业务 display_name）
    pub nickname: String,
    /// 头像 URL
    pub avatar_url: String,
    /// 当前用户给这个账号起的私有标签（社交业务里的好友备注）；人名解析链里排第一。
    #[serde(default)]
    pub alias: String,
}

impl UserProfile {
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            nickname: String::new(),
            avatar_url: String::new(),
            alias: String::new(),
        }
    }

    pub fn with_nickname(mut self, nickname: impl Into<String>) -> Self {
        self.nickname = nickname.into();
        self
    }

    pub fn with_avatar(mut self, avatar_url: impl Into<String>) -> Self {
        self.avatar_url = avatar_url.into();
        self
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = alias.into();
        self
    }

    /// 这个人的名字候选（别名、资料名）；会话内昵称与发送时快照由调用方补上。
    pub fn name_candidates(&self) -> crate::domain::PersonNameCandidates<'_> {
        crate::domain::PersonNameCandidates {
            alias: &self.alias,
            profile_name: &self.nickname,
            ..Default::default()
        }
    }

    /// 展示名：有 nickname 用 nickname，否则用 user_id（与 display_name 语义一致）
    pub fn display_name(&self) -> &str {
        if self.nickname.is_empty() {
            &self.user_id
        } else {
            &self.nickname
        }
    }
}
