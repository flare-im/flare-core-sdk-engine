/// Kernel port for tenant config notices (`DataPacket.tenant_notice`).
///
/// The downlink dispatcher hands each notice here; the SDK facade owns the
/// implementation: it merges the notices of a topic, waits a random part of the
/// notice's spread so a tenant's clients do not all re-read in the same
/// instant, then refreshes what depends on the topic (the plugin catalog) or
/// announces the topic to extensions.
pub trait TenantNoticePort: Send + Sync {
    fn notice(&self, notice: flare_proto::common::TenantNotice);
}
