use std::collections::HashMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::application::callbacks::{UploadPhase, UploadProgress, UploadProgressCallback};
use crate::domain::{MediaUploadManifestVo, MediaUploadPartVo};
use crate::infrastructure::transport::{
    SentBytesCallback, UploadFileHttpResponse, UploadFileMetadataHttp,
};
use crate::model::UploadedMedia;

/// 单次 PUT 的已发送字节 → `Uploading` 进度事件。没有进度订阅者时返回 `None`，
/// 传输层就走不计数的原路径。
pub(super) fn single_put_progress(
    on_progress: Option<&UploadProgressCallback>,
    file_name: &str,
    upload_id: &str,
    total_bytes: u64,
) -> Option<SentBytesCallback> {
    let on_progress = on_progress?.clone();
    let file_name = file_name.to_string();
    let upload_id = upload_id.to_string();
    Some(Arc::new(move |uploaded_bytes: u64| {
        on_progress(UploadProgress {
            file_name: file_name.clone(),
            upload_id: upload_id.clone(),
            phase: UploadPhase::Uploading,
            uploaded_bytes,
            total_bytes,
            chunk_index: Some(0),
            total_chunks: Some(1),
        });
    }))
}

pub(super) fn compute_bytes_fingerprints(data: &[u8]) -> (String, String, Option<String>) {
    let file_size = data.len() as u64;
    let head_len = usize::try_from(file_size.min(1024 * 1024)).unwrap_or(0);
    let head = &data[..head_len];
    let tail_len = usize::try_from(file_size.min(1024 * 1024)).unwrap_or(0);
    let tail = if tail_len == 0 {
        &[]
    } else {
        &data[data.len().saturating_sub(tail_len)..]
    };
    let full_sha256 = hex::encode(Sha256::digest(data));
    let head_hash = hex::encode(Sha256::digest(head));
    let tail_hash = hex::encode(Sha256::digest(tail));
    let head_tail_sha256 = hex::encode(Sha256::digest(
        format!("{head_hash}:{tail_hash}:{file_size}").as_bytes(),
    ));
    let fingerprint = hex::encode(Sha256::digest(
        format!("{file_size}:{head_hash}:{tail_hash}").as_bytes(),
    ));
    (fingerprint, head_tail_sha256, Some(full_sha256))
}

pub(super) fn build_upload_parts_from_manifest(
    manifest: &MediaUploadManifestVo,
) -> Vec<MediaUploadPartVo> {
    build_upload_parts(
        manifest.file_size,
        u64::from(manifest.part_size.max(1)),
        manifest.total_parts,
        &manifest.local_upload_id,
    )
}

pub(super) fn build_upload_parts(
    file_size: u64,
    part_size: u64,
    total_parts: u32,
    upload_id: &str,
) -> Vec<MediaUploadPartVo> {
    let total_parts = total_parts.max(1);
    let part_size = part_size.max(1);
    let mut parts = Vec::with_capacity(total_parts as usize);
    for idx in 0..total_parts {
        let part_number = idx + 1;
        let offset = u64::from(idx) * part_size;
        let remaining = file_size.saturating_sub(offset);
        let size = remaining.min(part_size);
        parts.push(MediaUploadPartVo {
            local_upload_id: upload_id.to_string(),
            part_number,
            offset,
            size,
            sha256: String::new(),
            etag: None,
            uploaded: false,
        });
    }
    parts
}

pub(super) fn build_control_headers(trace_seed: &str) -> HashMap<String, String> {
    HashMap::from([(
        "x-trace-id".to_string(),
        format!("sdk-upload-{trace_seed}-{}", rand::random::<u32>()),
    )])
}

pub(super) fn random_upload_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        chrono::Utc::now().timestamp_millis(),
        rand::random::<u32>()
    )
}

pub(super) fn upload_file_to_uploaded_media(
    data: UploadFileHttpResponse,
    fallback_file_name: String,
    fallback_mime_type: String,
    fallback_size: i64,
) -> UploadedMedia {
    let (resolved_name, resolved_mime, resolved_size) = if let Some(info) = data.info {
        (info.file_name, info.mime_type, info.size)
    } else {
        (fallback_file_name, fallback_mime_type, fallback_size)
    };
    UploadedMedia {
        file_id: data.file_id,
        file_name: resolved_name,
        mime_type: resolved_mime,
        size: resolved_size,
        url: data.url,
        cdn_url: data.cdn_url,
    }
}

pub(super) fn build_upload_metadata(
    file_name: String,
    mime_type: String,
    file_size: i64,
    file_type: i32,
    upload_id: String,
    user_id: String,
    purpose: crate::model::UploadPurpose,
) -> UploadFileMetadataHttp {
    UploadFileMetadataHttp {
        file_name,
        mime_type,
        file_size,
        file_type,
        upload_id: upload_id.clone(),
        metadata: if purpose == crate::model::UploadPurpose::Avatar {
            HashMap::from([("purpose".into(), "avatar".into())])
        } else {
            HashMap::new()
        },
        user_id,
        trace_id: upload_id,
        namespace: if purpose == crate::model::UploadPurpose::Avatar {
            "profile"
        } else {
            "im.message"
        }
        .into(),
        business_tag: if purpose == crate::model::UploadPurpose::Avatar {
            "avatar"
        } else {
            "chat_attachment"
        }
        .into(),
        bucket: String::new(),
        object_key: String::new(),
        labels: HashMap::new(),
    }
}

pub(super) fn bind_upload_purpose(
    fingerprint: String,
    purpose: crate::model::UploadPurpose,
) -> String {
    use sha2::{Digest as _, Sha256};
    if purpose == crate::model::UploadPurpose::General {
        fingerprint
    } else {
        format!(
            "{:x}",
            Sha256::digest(format!("avatar|{fingerprint}").as_bytes())
        )
    }
}

pub(super) fn infer_file_type(mime: &str) -> i32 {
    if mime.starts_with("image/") {
        1
    } else if mime.starts_with("video/") {
        2
    } else if mime.starts_with("audio/") {
        3
    } else if mime == "application/pdf"
        || mime.starts_with("application/")
        || mime.starts_with("text/")
    {
        4
    } else {
        5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_upload_parts_with_exact_offsets_and_tail_size() {
        let parts = build_upload_parts(10, 4, 3, "upload-1");

        assert_eq!(parts.len(), 3);
        assert_eq!(
            parts
                .iter()
                .map(|part| (part.part_number, part.offset, part.size))
                .collect::<Vec<_>>(),
            vec![(1, 0, 4), (2, 4, 4), (3, 8, 2)]
        );
        assert!(parts.iter().all(|part| part.local_upload_id == "upload-1"));
    }

    #[test]
    fn builds_upload_parts_from_manifest() {
        let manifest = MediaUploadManifestVo {
            local_upload_id: "manifest-1".to_string(),
            remote_upload_id: None,
            file_id: None,
            storage_upload_id: None,
            tenant_id: "0".to_string(),
            user_id: "u1".to_string(),
            source_kind: crate::domain::UploadSourceKind::StableFile,
            source_locator: "/tmp/a.bin".to_string(),
            file_name: "a.bin".to_string(),
            mime_type: "application/octet-stream".to_string(),
            file_size: 9,
            part_size: 4,
            total_parts: 3,
            transport_kind: None,
            bucket: None,
            object_key: None,
            upload_url: None,
            file_fingerprint: "fp".to_string(),
            head_tail_sha256: "ht".to_string(),
            full_sha256: None,
            state: crate::domain::UploadManifestState::Initiating,
            last_error_code: None,
            last_error_message: None,
            expires_at_ms: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };

        let parts = build_upload_parts_from_manifest(&manifest);

        assert_eq!(
            parts
                .iter()
                .map(|part| (
                    part.local_upload_id.as_str(),
                    part.part_number,
                    part.offset,
                    part.size
                ))
                .collect::<Vec<_>>(),
            vec![
                ("manifest-1", 1, 0, 4),
                ("manifest-1", 2, 4, 4),
                ("manifest-1", 3, 8, 1)
            ]
        );
    }

    #[test]
    fn computes_stable_byte_upload_fingerprints() {
        let (fingerprint_a, head_tail_a, full_a) = compute_bytes_fingerprints(b"flare-bytes");
        let (fingerprint_b, head_tail_b, full_b) = compute_bytes_fingerprints(b"flare-bytes");

        assert_eq!(fingerprint_a, fingerprint_b);
        assert_eq!(head_tail_a, head_tail_b);
        assert_eq!(full_a, full_b);
        assert_eq!(full_a.as_deref().map(str::len), Some(64));
    }

    #[test]
    fn infers_core_media_file_type_from_mime() {
        assert_eq!(infer_file_type("image/png"), 1);
        assert_eq!(infer_file_type("video/mp4"), 2);
        assert_eq!(infer_file_type("audio/mpeg"), 3);
        assert_eq!(infer_file_type("application/pdf"), 4);
        assert_eq!(infer_file_type("application/octet-stream"), 4);
        assert_eq!(infer_file_type("model/gltf+json"), 5);
    }
}

#[cfg(test)]
mod avatar_publication_tests {
    use super::*;
    use crate::model::UploadPurpose;
    #[test]
    fn avatar_scope_and_resume_identity_cannot_reuse_a_private_attachment() {
        let avatar = build_upload_metadata(
            "a.png".into(),
            "image/png".into(),
            10,
            1,
            "upload".into(),
            "u".into(),
            UploadPurpose::Avatar,
        );
        assert_eq!(avatar.namespace, "profile");
        assert_eq!(avatar.business_tag, "avatar");
        assert_eq!(avatar.metadata["purpose"], "avatar");
        assert_ne!(
            bind_upload_purpose("same-bytes".into(), UploadPurpose::Avatar),
            bind_upload_purpose("same-bytes".into(), UploadPurpose::General)
        );
        let private = build_upload_metadata(
            "a.png".into(),
            "image/png".into(),
            10,
            1,
            "upload".into(),
            "u".into(),
            UploadPurpose::General,
        );
        assert_eq!(private.namespace, "im.message");
        assert!(private.metadata.is_empty());
    }
}
