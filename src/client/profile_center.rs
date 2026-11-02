use serde::{Deserialize, Serialize};

use crate::domain::{DisplayName, NameEntity, UserProfile, resolve_person_name};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileCenterSummary {
    pub user_id: String,
    /// 「我」页显示的名字：人名解析链的结果，没有名字时为结构化的「无名」，不回落到 user_id。
    #[serde(default)]
    pub name: DisplayName,
    pub avatar_url: Option<String>,
    pub bio: Option<String>,
    pub im_connected: bool,
    pub contacts_count: u32,
    pub groups_count: u32,
    pub pending_requests_count: u32,
    pub warning: Option<String>,
}

impl ProfileCenterSummary {
    pub fn new(user_id: impl Into<String>) -> Self {
        let user_id = user_id.into();
        Self {
            name: DisplayName::unnamed(NameEntity::Person, &user_id),
            user_id,
            ..Self::default()
        }
    }

    /// 按身份缓存里本人的资料取名（人名解析链）；资料名为空或等于 user_id 时为「无名」。
    pub fn with_name_from(mut self, profile: &UserProfile) -> Self {
        self.name = resolve_person_name(&self.user_id, &profile.name_candidates(), None);
        self
    }

    pub fn normalized(mut self) -> Self {
        self.user_id = self.user_id.trim().to_owned();
        self.avatar_url = non_empty(self.avatar_url);
        self.bio = non_empty(self.bio);
        self.warning = non_empty(self.warning);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ProfileCenterActionKind {
    ViewProfile,
    EditProfile,
    PrivacySettings,
    SecuritySettings,
    StorageSettings,
    DeveloperDiagnostics,
    Logout,
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileCenterAction {
    pub key: String,
    pub label: String,
    pub kind: ProfileCenterActionKind,
    pub enabled: bool,
    pub destructive: bool,
}

impl ProfileCenterAction {
    pub fn new(
        key: impl Into<String>,
        label: impl Into<String>,
        kind: ProfileCenterActionKind,
    ) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            kind,
            enabled: true,
            destructive: false,
        }
    }

    pub fn destructive(mut self) -> Self {
        self.destructive = true;
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileCenterContract {
    pub summary: ProfileCenterSummary,
    pub actions: Vec<ProfileCenterAction>,
}

impl ProfileCenterContract {
    pub fn new(summary: ProfileCenterSummary) -> Self {
        Self {
            summary: summary.normalized(),
            actions: default_profile_center_actions(),
        }
    }
}

pub fn default_profile_center_actions() -> Vec<ProfileCenterAction> {
    vec![
        ProfileCenterAction::new(
            "profile.view",
            "View profile",
            ProfileCenterActionKind::ViewProfile,
        ),
        ProfileCenterAction::new(
            "profile.edit",
            "Edit profile",
            ProfileCenterActionKind::EditProfile,
        ),
        ProfileCenterAction::new(
            "privacy.settings",
            "Privacy settings",
            ProfileCenterActionKind::PrivacySettings,
        ),
        ProfileCenterAction::new(
            "security.settings",
            "Security settings",
            ProfileCenterActionKind::SecuritySettings,
        ),
        ProfileCenterAction::new(
            "storage.settings",
            "Storage settings",
            ProfileCenterActionKind::StorageSettings,
        ),
        ProfileCenterAction::new(
            "developer.diagnostics",
            "Developer diagnostics",
            ProfileCenterActionKind::DeveloperDiagnostics,
        ),
        ProfileCenterAction::new("session.logout", "Log out", ProfileCenterActionKind::Logout)
            .destructive(),
    ]
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|item| item.trim().to_owned())
        .filter(|item| !item.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_normalization_trims_and_never_names_the_user_by_id() {
        let summary = ProfileCenterSummary {
            avatar_url: Some(" ".to_owned()),
            bio: Some(" hello ".to_owned()),
            ..ProfileCenterSummary::new(" user-1 ")
        }
        .normalized();

        assert_eq!(summary.user_id, "user-1");
        assert_eq!(
            summary.name,
            DisplayName::unnamed(NameEntity::Person, "user-1"),
            "the name never falls back to the user id"
        );
        assert_eq!(summary.avatar_url, None);
        assert_eq!(summary.bio.as_deref(), Some("hello"));
    }

    #[test]
    fn summary_name_follows_the_person_name_chain() {
        let named = ProfileCenterSummary::new("user-1")
            .with_name_from(&UserProfile::new("user-1").with_nickname(" Hugo "));
        assert_eq!(
            named.name,
            DisplayName::named("Hugo", crate::domain::NameSource::Profile)
        );
        let id_named = ProfileCenterSummary::new("user-1")
            .with_name_from(&UserProfile::new("user-1").with_nickname("user-1"));
        assert_eq!(
            id_named.name,
            DisplayName::unnamed(NameEntity::Person, "user-1")
        );
    }

    #[test]
    fn default_contract_contains_destructive_logout_action() {
        let contract = ProfileCenterContract::new(ProfileCenterSummary::new("u1"));
        let logout = contract
            .actions
            .iter()
            .find(|action| action.key == "session.logout")
            .expect("logout action");

        assert!(logout.enabled);
        assert!(logout.destructive);
        assert_eq!(logout.kind, ProfileCenterActionKind::Logout);
    }
}
