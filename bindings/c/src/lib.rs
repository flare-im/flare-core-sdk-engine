//! C ABI for Flare IM Core SDK.
//!
//! The crate root intentionally contains no IM behavior. It wires the
//! production ABI modules so generated contract entrypoints are compiled and
//! exported as the native L1 boundary.

mod abi;
mod dispatch_common;
mod error_convert;
mod event;
mod executor;
mod ffi_runtime;
pub mod generated;
mod helpers;
mod lifecycle;
mod registry;
mod session;
mod token_refresh;
pub mod types;

pub use types::{
    FlareBytes, FlareBytesView, FlareError, FlareEventBatchCallback, FlareEventCallback,
    FlareHandle, FlareProgressCallback, FlareResultCallback, FlareString, FlareStringView,
    FlareSubscriptionHandle, FlareTaskHandle, FlareTokenRefreshCallback,
};
