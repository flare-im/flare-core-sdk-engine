//! What reaches an open bundle besides answers: the events it subscribed to
//! (`theme.changed`, `locale.changed`, `visibility.changed`,
//! `catalog.changed`, `entity.state`) and `lifecycle.revoked` when the catalog
//! stops serving what it opened. Also what a successful catalog read means
//! for the cache: which digests are still named, and which entry bundles are
//! worth fetching ahead on Wi-Fi.
use std::collections::BTreeSet;

use flare_plugin_protocol::bridge::{
    BridgeEnvelope, BridgeEvent, BridgeTheme, EntityStatePayload, LocaleChangedPayload,
    RevokeReason, VisibilityChangedPayload,
};
use flare_plugin_protocol::bundle::WebBundleDecl;
use flare_plugin_protocol::manifest::UiContributionKind;
use flare_plugin_protocol::state::SharedState;
use flare_plugin_protocol::ui::{UiActionTarget, UiCatalog};
use serde_json::{Value, json};

use super::broker::SessionContext;

/// An event the host reports (`plugin_bundle_host_event`): the theme, the
/// language or whether the page is visible changed.
pub fn host_event(event: &str, payload: &Value) -> Option<(BridgeEvent, Value)> {
    let topic = BridgeEvent::parse(event)?;
    let payload = match topic {
        BridgeEvent::ThemeChanged => {
            serde_json::to_value(BridgeTheme::read(payload.clone())?).ok()?
        }
        BridgeEvent::LocaleChanged => {
            json!({ "locale": LocaleChangedPayload::read(payload.clone())?.locale })
        }
        BridgeEvent::VisibilityChanged => {
            json!({ "visible": VisibilityChangedPayload::read(payload.clone())?.visible })
        }
        // The engine's own: the catalog, the lifecycle, shared entity state.
        BridgeEvent::CatalogChanged | BridgeEvent::LifecycleRevoked | BridgeEvent::EntityState => {
            return None;
        }
    };
    Some((topic, payload))
}

pub fn envelope(topic: BridgeEvent, payload: Value) -> BridgeEnvelope {
    BridgeEnvelope::event(topic, payload)
}

pub fn catalog_changed() -> BridgeEnvelope {
    envelope(BridgeEvent::CatalogChanged, json!({}))
}

/// The `entity.state` event of a card's shared state, for a session of the
/// plugin that posted the card in the session's conversation.
pub fn entity_state(
    context: &SessionContext,
    card_plugin: &str,
    conversation_id: &str,
    state: &SharedState,
) -> Option<BridgeEnvelope> {
    (context.plugin_id == card_plugin
        && context.conversation_id.as_deref() == Some(conversation_id))
    .then(|| {
        envelope(
            BridgeEvent::EntityState,
            serde_json::to_value(EntityStatePayload {
                entity: state.entity.clone(),
                version: state.version,
                values: state.values.clone(),
            })
            .unwrap_or_default(),
        )
    })
}

/// Why `catalog` no longer serves what `context` opened, or `None` when it
/// still does: the plugin left the catalog (disabled, uninstalled, suspended
/// for the viewer), the plugin moved to another version, the bundle was
/// withdrawn or replaced by other bytes, or the contribution stopped opening
/// it.
pub fn revocation(context: &SessionContext, catalog: &UiCatalog) -> Option<RevokeReason> {
    let Some(plugin) = catalog
        .plugins
        .iter()
        .find(|plugin| plugin.plugin_id == context.plugin_id)
    else {
        return Some(RevokeReason::Disabled);
    };
    if plugin.version != context.plugin_version {
        return Some(RevokeReason::Replaced);
    }
    let opened = plugin
        .extensions
        .contributions
        .iter()
        .find(|contribution| contribution.id == context.contribution_id)
        .and_then(|contribution| contribution.bundle.as_ref())
        .is_some_and(|bundle| bundle.id == context.bundle_id);
    match plugin.extensions.web_bundle(&context.bundle_id) {
        None => Some(RevokeReason::Withdrawn),
        Some(entry) if entry.sha256 != context.sha256 => Some(RevokeReason::Replaced),
        Some(_) if !opened => Some(RevokeReason::Withdrawn),
        Some(_) => None,
    }
}

/// The digests a catalog names: the cache keeps these.
pub fn referenced_digests(catalog: &UiCatalog) -> BTreeSet<String> {
    catalog
        .plugins
        .iter()
        .flat_map(|plugin| plugin.extensions.web_bundles.iter())
        .map(|bundle| bundle.sha256.clone())
        .collect()
}

/// Bundles worth fetching ahead (design 8.3: on Wi-Fi, when idle): those an
/// entry opens, a "+" form itself or the page a "Me" entry or a "+" action
/// leads to, by plugin.
pub fn prefetch_candidates(catalog: &UiCatalog) -> Vec<(String, WebBundleDecl)> {
    let mut out = Vec::new();
    for plugin in &catalog.plugins {
        let contributions = &plugin.extensions.contributions;
        let entry_pages: BTreeSet<&str> = contributions
            .iter()
            .filter(|c| {
                matches!(
                    c.kind,
                    UiContributionKind::AccountEntry | UiContributionKind::ComposerAction
                )
            })
            .flat_map(|c| c.schema.actions.iter())
            .filter_map(|action| match &action.target {
                UiActionTarget::Page { page, plugin_id }
                    if plugin_id.as_deref().is_none_or(|id| id == plugin.plugin_id) =>
                {
                    Some(page.as_str())
                }
                _ => None,
            })
            .collect();
        let wanted: BTreeSet<&str> = contributions
            .iter()
            .filter(|c| {
                c.kind == UiContributionKind::ComposerAction
                    || (c.kind == UiContributionKind::Page && entry_pages.contains(c.id.as_str()))
            })
            .filter_map(|c| c.bundle.as_ref().map(|bundle| bundle.id.as_str()))
            .collect();
        for bundle in &plugin.extensions.web_bundles {
            if wanted.contains(bundle.id.as_str()) {
                out.push((plugin.plugin_id.clone(), bundle.clone()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "abababababababababababababababababababababababababababababababab";

    fn catalog(version: &str, sha256: &str, bundle_on_page: bool, entry: bool) -> UiCatalog {
        let mut page = json!({"kind": "page", "id": "example.board.page", "schemaVersion": 1,
            "requiredPermissions": [],
            "schema": {"title": "Board", "fields": [], "actions": []}});
        if bundle_on_page {
            page["bundle"] = json!({"id": "board", "route": "/"});
            page["variant"] = json!("board");
        }
        let mut contributions = vec![page];
        if entry {
            contributions.push(json!({"kind": "account-entry", "id": "example.board.entry",
                "schemaVersion": 1, "requiredPermissions": [],
                "schema": {"title": "Board", "fields": [], "actions": [{"id": "open",
                    "label": "Open", "target": {"kind": "page", "page": "example.board.page"}}]}}));
        }
        serde_json::from_value(json!({
            "tenantId": "t1", "version": 3, "validUntilMs": 1,
            "plugins": [{
                "pluginId": "example.board", "version": version, "manifestDigest": "sha256:00",
                "grantRevision": 1,
                "extensions": {"platforms": ["web"], "contributions": contributions,
                    "webBundles": [{"id": "board", "sha256": sha256, "size": 10, "bridge": 1,
                        "urls": ["https://m.example.com/b.fwb"]}]},
                "operations": []
            }],
            "unavailable": []
        }))
        .unwrap()
    }

    fn context() -> SessionContext {
        SessionContext {
            tenant_id: "t1".into(),
            viewer: "u1".into(),
            plugin_id: "example.board".into(),
            plugin_version: "1.0.0".into(),
            contribution_id: "example.board.page".into(),
            route: "/".into(),
            bundle_id: "board".into(),
            sha256: DIGEST.into(),
            conversation_id: Some("c1".into()),
        }
    }

    #[test]
    fn an_open_page_is_revoked_when_the_catalog_stops_serving_its_bundle() {
        let ctx = context();
        assert_eq!(
            revocation(&ctx, &catalog("1.0.0", DIGEST, true, false)),
            None
        );
        // Withdrawn: Core selects the declarative variant, no bundle listed.
        let mut withdrawn = catalog("1.0.0", DIGEST, false, false);
        withdrawn.plugins[0].extensions.web_bundles.clear();
        assert_eq!(revocation(&ctx, &withdrawn), Some(RevokeReason::Withdrawn));
        // The contribution no longer opens it.
        assert_eq!(
            revocation(&ctx, &catalog("1.0.0", DIGEST, false, false)),
            Some(RevokeReason::Withdrawn)
        );
        // Other bytes under the same id, or another plugin version.
        assert_eq!(
            revocation(&ctx, &catalog("1.0.0", &"cd".repeat(32), true, false)),
            Some(RevokeReason::Replaced)
        );
        assert_eq!(
            revocation(&ctx, &catalog("1.1.0", DIGEST, true, false)),
            Some(RevokeReason::Replaced)
        );
        // The plugin left the catalog (disabled or suspended for the tenant).
        let mut gone = catalog("1.0.0", DIGEST, true, false);
        gone.plugins.clear();
        assert_eq!(revocation(&ctx, &gone), Some(RevokeReason::Disabled));
    }

    #[test]
    fn digests_and_prefetch_follow_the_catalog() {
        let with_entry = catalog("1.0.0", DIGEST, true, true);
        assert_eq!(referenced_digests(&with_entry), [DIGEST.to_owned()].into());
        let candidates = prefetch_candidates(&with_entry);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, "example.board");
        assert!(prefetch_candidates(&catalog("1.0.0", DIGEST, true, false)).is_empty());
    }

    #[test]
    fn host_events_are_typed_and_engine_topics_are_not_the_hosts() {
        assert!(host_event("theme.changed", &json!({"mode": "dark", "tokens": {}})).is_some());
        assert!(host_event("theme.changed", &json!({"mode": "sepia"})).is_none());
        assert!(host_event("visibility.changed", &json!({"visible": false})).is_some());
        assert!(host_event("lifecycle.revoked", &json!({"reason": "withdrawn"})).is_none());
        assert!(host_event("catalog.changed", &json!({})).is_none());
        assert!(host_event("other", &json!({})).is_none());
    }

    #[test]
    fn entity_state_goes_to_the_plugins_pages_in_the_same_conversation() {
        let state = SharedState {
            entity: "task-1".into(),
            version: 4,
            values: serde_json::from_value(json!({"done": true})).unwrap(),
        };
        let event = entity_state(&context(), "example.board", "c1", &state).unwrap();
        assert_eq!(
            event.payload,
            Some(json!({"entity": "task-1", "version": 4, "values": {"done": true}}))
        );
        assert!(entity_state(&context(), "example.other", "c1", &state).is_none());
        assert!(entity_state(&context(), "example.board", "c2", &state).is_none());
    }
}
