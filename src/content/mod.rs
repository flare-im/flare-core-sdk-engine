//! Message content construction, decoding, preview, and rich document support.

pub mod content_builder;
pub mod decoder;
pub mod mention;
pub mod message_builder;
pub mod message_elem;
pub mod preview_storage;
pub mod rich_doc_v2;
pub mod url_safety;

pub use content_builder::{BuiltContent, ContentBuilder, DEFAULT_STICKER_DISPLAY_SIDE};
pub use decoder::{DecodedContent, decode_content, decode_content_bytes};
pub use mention::{MentionCandidate, ParsedMentions, mentions_from_content, parse_mentions};
pub(crate) use mention::{TextMention, resolve_mentions};
pub use message_builder::MessageBuilder;
pub use message_elem::{Elem, MessagePreviewElem, decoded_content_to_elem};
pub use preview_storage::PreviewStoragePayload;
pub use rich_doc_v2::{
    NormalizeOutput, RichDocDerived, RichDocV2Error, derive_from_json_str, derive_from_value,
    normalize_from_doc_json, normalize_from_html, normalize_from_markdown, validate_doc_json,
};
