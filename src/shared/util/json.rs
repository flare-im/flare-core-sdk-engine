//! Reading JSON text into a type.
//!
//! In the browser bundle text is read into a [`serde_json::Value`] first and
//! the type from that value. Most types the SDK reads from text it also reads
//! from values (binding requests, catalogs, events), and serde compiles a
//! reader of a type for each input it reads from: going through the value
//! leaves one reader per type instead of two. Native builds read the text
//! directly, which is faster and allocates less.
//!
//! What reads differently through a value: a key repeated in an object keeps
//! its last value instead of failing, and errors carry no line and column.
//! The SDK reads only text it or its servers wrote.
//!
//! Code the browser bundle runs reads text with `serde_json::from_slice`, never
//! `from_str`: the two compile separate parsers that read the same JSON the same
//! way, with the same errors, so using one of them keeps the other out.
use serde::de::DeserializeOwned;

/// Read `T` from JSON text.
pub fn from_str<T: DeserializeOwned>(text: &str) -> serde_json::Result<T> {
    #[cfg(target_arch = "wasm32")]
    {
        from_value(serde_json::from_slice(text.as_bytes())?)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        serde_json::from_str(text)
    }
}

/// Read `T` from JSON bytes.
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> serde_json::Result<T> {
    #[cfg(target_arch = "wasm32")]
    {
        from_value(serde_json::from_slice(bytes)?)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        serde_json::from_slice(bytes)
    }
}

/// Read `T` from a JSON value, a struct from an object only
/// ([`flare_plugin_protocol::json`], the one value reader of a client).
pub fn from_value<T: DeserializeOwned>(value: serde_json::Value) -> serde_json::Result<T> {
    flare_plugin_protocol::json::from_value(value)
}
