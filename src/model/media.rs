#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaAccessUrl {
    pub url: String,
    pub cdn_url: Option<String>,
}

/// [`crate::platform::ports::media::MediaServicePort::resolve_media_access`] 结果：优先本地缓存，否则返回短时远程地址。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaResolvedAccess {
    /// `"local"` 或 `"remote"`
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<MediaAccessUrl>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum MediaDestinationKind {
    InlineDisplay,
    SaveToDevice,
    Path,
    Bytes,
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct MediaDestinationDescriptor {
    pub kind: MediaDestinationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl MediaDestinationDescriptor {
    pub fn inline_display() -> Self {
        Self {
            kind: MediaDestinationKind::InlineDisplay,
            path: None,
        }
    }

    pub fn save_to_device() -> Self {
        Self {
            kind: MediaDestinationKind::SaveToDevice,
            path: None,
        }
    }

    pub fn path(path: impl Into<String>) -> Self {
        Self {
            kind: MediaDestinationKind::Path,
            path: Some(path.into()),
        }
    }

    pub fn bytes() -> Self {
        Self {
            kind: MediaDestinationKind::Bytes,
            path: None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum RenderableMediaKind {
    LocalPath,
    RemoteUrl,
    ObjectUrl,
    Bytes,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RenderableMedia {
    pub file_id: String,
    pub destination: MediaDestinationDescriptor,
    pub kind: RenderableMediaKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub render_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<MediaAccessUrl>,
}

impl RenderableMedia {
    pub fn from_resolved_access(file_id: impl Into<String>, resolved: MediaResolvedAccess) -> Self {
        if let Some(local_path) = resolved.local_path.filter(|path| !path.trim().is_empty()) {
            return Self {
                file_id: file_id.into(),
                destination: MediaDestinationDescriptor::inline_display(),
                kind: RenderableMediaKind::LocalPath,
                render_url: Some(local_path.clone()),
                local_path: Some(local_path),
                remote: None,
            };
        }

        let render_url = resolved.remote.as_ref().map(preferred_media_access_url);
        Self {
            file_id: file_id.into(),
            destination: MediaDestinationDescriptor::inline_display(),
            kind: RenderableMediaKind::RemoteUrl,
            render_url,
            local_path: None,
            remote: resolved.remote,
        }
    }
}

fn preferred_media_access_url(access: &MediaAccessUrl) -> String {
    access
        .cdn_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| access.url.trim())
        .to_string()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UploadedMedia {
    pub file_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub size: i64,
    pub url: Option<String>,
    pub cdn_url: Option<String>,
}

/// Explicit publication intent. Only validated avatar images become public.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum UploadPurpose {
    #[default]
    General,
    Avatar,
}
impl UploadPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::Avatar => "avatar",
        }
    }
}
impl std::str::FromStr for UploadPurpose {
    type Err = ();
    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw {
            "general" => Ok(Self::General),
            "avatar" => Ok(Self::Avatar),
            _ => Err(()),
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct UploadOptions {
    pub chunk_size: usize,
    pub purpose: UploadPurpose,
}

impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            chunk_size: 256 * 1024,
            purpose: UploadPurpose::General,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_destination_descriptor_is_typed_and_supports_path_payload() {
        let inline = MediaDestinationDescriptor::inline_display();
        assert_eq!(inline.kind, MediaDestinationKind::InlineDisplay);
        assert!(inline.path.is_none());

        let path = MediaDestinationDescriptor::path("/tmp/photo.jpg");
        assert_eq!(path.kind, MediaDestinationKind::Path);
        assert_eq!(path.path.as_deref(), Some("/tmp/photo.jpg"));

        let json = serde_json::to_value(MediaDestinationDescriptor::save_to_device())
            .expect("destination json");
        assert_eq!(json["kind"], "saveToDevice");
        assert!(json.get("path").is_none());
    }
}
