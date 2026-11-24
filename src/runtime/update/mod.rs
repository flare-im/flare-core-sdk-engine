//! Application update provider used by the ABI and client lifecycle.

use std::sync::Arc;

use flare_access_contract::update::{UpdateInstallPlan, UpdateResources, UpdateStatus};
use tokio::sync::broadcast;

use crate::{Result, runtime::link::ConnectionRuntime};

pub(crate) type UpdateRuntime = dyn UpdateService;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait UpdateService: Send + Sync {
    fn upgrade_required(self: Arc<Self>, min_build: Option<u64>);
    fn release_notice_handler(self: Arc<Self>) -> Arc<dyn Fn() + Send + Sync>;
    fn subscribe(&self) -> broadcast::Receiver<UpdateStatus>;
    fn status_now(&self) -> UpdateStatus;
    fn foregrounded(self: Arc<Self>);
    fn backgrounded(&self);
    fn network_changed(self: Arc<Self>, available: bool, metered: bool);
    fn request_check(self: Arc<Self>, manual: bool) -> UpdateStatus;
    async fn state(self: Arc<Self>, foreground: bool) -> UpdateStatus;
    async fn set_locale(&self, locale: String) -> UpdateStatus;
    async fn resources(&self) -> UpdateResources;
    async fn download(self: Arc<Self>, allow_metered: bool) -> UpdateStatus;
    async fn install(&self) -> Result<UpdateInstallPlan>;
    async fn report_install(&self, outcome: &str, code: Option<&str>) -> Result<UpdateStatus>;
    async fn defer(&self) -> UpdateStatus;
    async fn skip(&self, build: u64) -> UpdateStatus;
    async fn mark_prompted(&self, build: Option<u64>, form: &str) -> Result<UpdateStatus>;
    async fn release_at_least(&self, min_build: u64) -> Option<u64>;
    async fn open_flow(&self) -> UpdateStatus;
}

pub(crate) fn observe_upgrade(connection: &ConnectionRuntime, update: &Arc<UpdateRuntime>) {
    let provider = Arc::downgrade(update);
    connection.observe_upgrade_required(Arc::new(move |min_build| {
        if let Some(provider) = provider.upgrade() {
            provider.upgrade_required(min_build);
        }
    }));
}
