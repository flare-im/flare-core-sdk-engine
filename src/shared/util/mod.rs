#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod atomic_file;
pub mod background;
pub mod constants;
pub mod date;
pub mod id;
pub mod media_duration;
pub mod paths;
#[cfg(feature = "lifecycle-sqlite")]
pub mod sqlite_store;
pub mod time;
pub mod token;
pub(crate) use token::now_unix_secs;

pub const DEFAULT_TENANT_ID: &str = "0";

#[inline]
pub fn normalize_tenant_id(tenant_id: impl AsRef<str>) -> String {
    match tenant_id.as_ref().trim() {
        "" | "default" => DEFAULT_TENANT_ID.to_string(),
        tenant_id => tenant_id.to_string(),
    }
}

pub use background::{BackgroundTask, spawn_background, spawn_background_task};
pub use constants::{
    RELIABLE_QUEUE_MAX_IN_FLIGHT, RELIABLE_QUEUE_MAX_RETRIES, RELIABLE_QUEUE_TIMEOUT_SECS,
    REQUEST_TIMEOUT_SECS, WAIT_ACK_TIMEOUT_SECS,
};
pub use date::{
    ms_to_prost_timestamp, ms_to_rfc3339, prost_timestamp_to_ms, prost_timestamp_to_rfc3339,
    prost_timestamp_to_rfc3339_parts, rfc3339_to_prost_timestamp, system_time_to_prost_timestamp,
};
pub use paths::{
    default_sdk_data_root, dev_data_dir_relative_to_cwd, parse_data_url_to_path,
    resolve_sdk_data_root, resolve_user_db_path, resolve_user_media_cache_dir,
    sanitize_user_id_for_dir,
};
#[cfg(feature = "lifecycle-sqlite")]
pub use sqlite_store::{
    open_sqlite_store_for_user, open_sqlite_store_for_user_with_secure_key_store,
    open_sqlite_store_for_user_with_security, open_sqlite_store_provider,
    open_sqlite_store_provider_with_security, sqlite_database_url_from_path,
    sqlite_security_config_from_secure_key_store,
};
pub use time::{
    TimeoutElapsed, deadline_after, delay, is_deadline_elapsed, now_millis, timeout,
    yield_for_frame,
};
pub use token::{CoreTokenConfig, generate_core_token};
