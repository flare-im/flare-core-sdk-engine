//! Read-only recovery of a stored page intent whose original write is absent.
use super::*;
use crate::{
    client::api::page_command::PageCommandCall,
    domain::{PluginCommand, PluginCommandScope, PluginCommandStore},
    infrastructure::persistence::sqlite::{SqlitePluginCommandRepo, init_schema},
};
use flare_plugin_protocol::{
    runtime::configuration_digest,
    ui::{UiPageCommand, UiPageCommandMode},
};

const SLOT: &str = "example.poll.workflow";
const QUERY: &str = "example.poll.observe";

struct Pending {
    f: Fixture,
    store: Arc<SqlitePluginCommandRepo>,
    command: PluginCommand,
    scope: PluginCommandScope,
}
impl Pending {
    async fn new() -> Self {
        let mut f = Fixture::new().await;
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        init_schema(&pool).await.unwrap();
        let store = Arc::new(SqlitePluginCommandRepo::new(pool));
        f.api = f.api.clone().with_plugin_command_store(Some(store.clone()));
        let command: PluginCommand = serde_json::from_value(json!({
            "commandId":"original-id", "capabilityId":"example.poll.removed.write",
            "conversationId":"conversation-1", "payload":{"commandId":"original-id","decimal":"1.0000000000000000001"},
            "terminalErrorCodes":["DECLINED"],
            "continuation":{"page":"example.poll.original-result","input":{}}
        })).unwrap();
        let scope = PluginCommandScope {
            tenant_id: "201".into(),
            user_id: "u1".into(),
            slot: SLOT.into(),
        };
        store.prepare(&scope, &command).await.unwrap();
        {
            let mut catalog = f.catalog.lock().unwrap();
            catalog.plugins[0].operations.insert(QUERY.into());
            catalog.plugins[0].query_operations.insert(QUERY.into());
            catalog.plugins[0].extensions.contributions.push(
                serde_json::from_value(json!({
                    "kind":"page", "id":"example.poll.recovery", "schemaVersion":1,"requiredPermissions":[],
                    "schema":{"title":"Recovery","actions":[{
                        "id":"observe", "label":"Check original operation",
                        "input":{"callerOverride":"/override"},
                        "target":{"kind":"operation","operation":QUERY},
                        "command":{"slot":SLOT,"mode":"observe"},
                        "continuation":{"page":"example.poll.observer-result","input":{}}
                    }]}
                }))
                .unwrap(),
            );
            for page in [
                "example.poll.original-result",
                "example.poll.observer-result",
            ] {
                catalog.plugins[0].extensions.contributions.push(
                    serde_json::from_value(json!({
                        "kind":"page","id":page,"schemaVersion":1,"requiredPermissions":[],"schema":{"title":"Result"}
                    }))
                    .unwrap(),
                );
            }
        }
        Self {
            f,
            store,
            command,
            scope,
        }
    }
    fn catalog(&self) -> UiCatalog {
        self.f.catalog.lock().unwrap().clone()
    }
    fn policy() -> UiPageCommand {
        UiPageCommand {
            slot: SLOT.into(),
            mode: UiPageCommandMode::Observe,
            terminal_error_codes: vec![],
        }
    }
    async fn observe(
        &self,
        catalog: &UiCatalog,
        viewer: &str,
    ) -> Result<(
        CapabilityDispatchResult,
        Option<flare_plugin_protocol::ui::UiPageTransition>,
    )> {
        self.f
            .api
            .page_command(PageCommandCall {
                plugin: "example.poll",
                catalog,
                viewer,
                operation: QUERY,
                payload: json!({"commandId":"forged","payload":{"decimal":"999"}}),
                conversation: None,
                policy: &Self::policy(),
                continuation: None,
            })
            .await
    }
    fn observation(&self, result: Value) -> Value {
        json!({"commandId":self.command.command_id,"capabilityId":self.command.capability_id,
            "requestDigest":configuration_digest(&self.command.payload).unwrap(),
            "outcome":"committed","result":result})
    }
    async fn assert_unknown(&self) {
        let stored = self.store.pending(&self.scope).await.unwrap().unwrap();
        assert_eq!(stored.command, self.command);
        assert!(stored.response.is_none());
        assert!(
            self.store
                .acknowledge(&self.scope, &self.command.command_id)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn observes_original_payload_through_current_declared_query_without_a_write() {
    let p = Pending::new().await;
    let value = p
        .f
        .api
        .invoke_plugin_action(json!({
            "page":{"pluginId":"example.poll","page":"example.poll.recovery"},
            "platform":"web","actionId":"observe","userGesture":true,
            "data":{"_session":{"tenantId":"201","userId":"u1"},"override":{"commandId":"forged"}}
        }))
        .await
        .unwrap();
    assert_eq!(value["kind"], "host");
    assert_eq!(value["target"]["page"], "example.poll.original-result");
    let calls = p.f.calls.lock().unwrap();
    let queries: Vec<_> = calls
        .iter()
        .filter(|c| c["capabilityId"] != flare_plugin_protocol::ui::UI_CATALOG_OPERATION)
        .collect();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0]["capabilityId"], QUERY);
    assert_eq!(
        queries[0]["payload"],
        json!({"commandId":p.command.command_id,"capabilityId":p.command.capability_id,"payload":p.command.payload})
    );
    drop(calls);
    let stored = p.store.pending(&p.scope).await.unwrap().unwrap();
    assert_eq!(stored.command, p.command);
    let response = stored.response.unwrap();
    assert_eq!(response["success"], true);
    assert_eq!(response["capabilityId"], p.command.capability_id);
}

#[tokio::test]
async fn invalid_identity_digest_and_not_found_never_complete_or_ack() {
    for kind in [
        "command",
        "capability",
        "digest",
        "notFound",
        "missing",
        "wrongShape",
        "unknownRefusal",
    ] {
        let p = Pending::new().await;
        let mut observed = p.observation(json!({"success":true,"data":{"status":"recorded"}}));
        match kind {
            "command" => observed["commandId"] = json!("other"),
            "capability" => observed["capabilityId"] = json!("example.other.write"),
            "digest" => observed["requestDigest"] = json!("sha256:wrong"),
            "notFound" => {
                observed["outcome"] = json!("notFound");
                observed["result"] = Value::Null;
            }
            "missing" => observed["result"] = Value::Null,
            "wrongShape" => observed["result"] = json!({"status":"recorded"}),
            "unknownRefusal" => {
                observed["result"] = json!({"success":false,"data":{"errorCode":"UNKNOWN"}})
            }
            _ => unreachable!(),
        }
        *p.f.observation.lock().unwrap() = Some(observed);
        assert!(p.observe(&p.catalog(), "u1").await.is_err(), "{kind}");
        p.assert_unknown().await;
    }
}

#[tokio::test]
async fn query_descriptor_authorization_expiry_and_identity_are_fail_closed() {
    let p = Pending::new().await;
    for kind in ["noDescriptor", "notAuthorized", "expired", "tenant"] {
        let mut catalog = p.catalog();
        match kind {
            "noDescriptor" => catalog.plugins[0].query_operations.clear(),
            "notAuthorized" => {
                catalog.plugins[0].operations.remove(QUERY);
            }
            "expired" => catalog.valid_until_ms = 0,
            "tenant" => catalog.tenant_id = "other".into(),
            _ => unreachable!(),
        }
        assert!(p.observe(&catalog, "u1").await.is_err(), "{kind}");
    }
    assert!(p.observe(&p.catalog(), "other-user").await.is_err());
    assert!(
        p.f.calls.lock().unwrap().is_empty(),
        "no query or write may escape denial"
    );
    p.assert_unknown().await;
}

#[tokio::test]
async fn known_terminal_rejection_is_preserved_and_reused_without_network() {
    let p = Pending::new().await;
    *p.f.observation.lock().unwrap() =
        Some(p.observation(json!({"success":false,"data":{"errorCode":"DECLINED"}})));
    let value =
        p.f.api
            .invoke_plugin_action(json!({
                "page":{"pluginId":"example.poll","page":"example.poll.recovery"},
                "platform":"web","actionId":"observe","userGesture":true,
                "data":{"_session":{"tenantId":"201","userId":"u1"},"override":{}}
            }))
            .await
            .unwrap();
    assert_eq!(value["kind"], "operation", "a rejection must not navigate");
    let response: CapabilityDispatchResult =
        serde_json::from_value(value["response"].clone()).unwrap();
    assert!(!response.success);
    assert_eq!(response.data["errorCode"], "DECLINED");
    assert_eq!(response.capability_id, p.command.capability_id);
    let count = p.f.calls.lock().unwrap().len();
    let (again, continuation) = p.observe(&p.catalog(), "u1").await.unwrap();
    assert_eq!(continuation, p.command.continuation);
    assert_eq!(
        serde_json::to_value(again).unwrap(),
        serde_json::to_value(response).unwrap()
    );
    assert_eq!(p.f.calls.lock().unwrap().len(), count);
    p.store
        .acknowledge(&p.scope, &p.command.command_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn transport_failure_leaves_the_original_unknown() {
    let p = Pending::new().await;
    p.f.drop_next.store(true, Ordering::SeqCst);
    assert!(p.observe(&p.catalog(), "u1").await.is_err());
    p.assert_unknown().await;
}

#[tokio::test]
async fn a_wrong_outer_plugin_or_query_identity_cannot_settle_an_original_command() {
    for identity in [
        ("example.other".into(), QUERY.into()),
        ("example.poll".into(), "example.poll.other-query".into()),
    ] {
        let p = Pending::new().await;
        *p.f.observation_identity.lock().unwrap() = Some(identity);
        assert!(
            p.f.api
                .invoke_plugin_action(json!({
                    "page":{"pluginId":"example.poll","page":"example.poll.recovery"},
                    "platform":"web","actionId":"observe","userGesture":true,
                    "data":{"_session":{"tenantId":"201","userId":"u1"},"override":{}}
                }))
                .await
                .is_err()
        );
        p.assert_unknown().await;
    }
}

#[tokio::test]
async fn removing_the_current_observe_declaration_prevents_any_query() {
    let p = Pending::new().await;
    p.f.catalog.lock().unwrap().plugins[0]
        .extensions
        .contributions
        .iter_mut()
        .find(|page| page.id == "example.poll.recovery")
        .unwrap()
        .schema
        .actions
        .clear();
    assert!(
        p.f.api
            .invoke_plugin_action(json!({
                "page":{"pluginId":"example.poll","page":"example.poll.recovery"},
                "platform":"web","actionId":"observe","userGesture":true,
                "data":{"_session":{"tenantId":"201","userId":"u1"},"override":{}}
            }))
            .await
            .is_err()
    );
    assert!(
        p.f.calls
            .lock()
            .unwrap()
            .iter()
            .all(|c| c["capabilityId"] == flare_plugin_protocol::ui::UI_CATALOG_OPERATION)
    );
    p.assert_unknown().await;
}

#[tokio::test]
async fn a_stale_observation_cannot_complete_a_replaced_command() {
    let p = Pending::new().await;
    p.f.hold.store(true, Ordering::SeqCst);
    let api = p.f.api.clone();
    let catalog = p.catalog();
    let work = tokio::spawn(async move {
        api.page_command(PageCommandCall {
            plugin: "example.poll",
            catalog: &catalog,
            viewer: "u1",
            operation: QUERY,
            payload: json!({}),
            conversation: None,
            policy: &Pending::policy(),
            continuation: None,
        })
        .await
    });
    p.f.entered.notified().await;
    p.store.complete(&p.scope, &p.command, json!({"requestId":"known","success":true,"pluginId":"example.poll","capabilityId":p.command.capability_id,"data":{},"error":null})).await.unwrap();
    p.store
        .acknowledge(&p.scope, &p.command.command_id)
        .await
        .unwrap();
    let mut replacement = p.command.clone();
    replacement.command_id = "replacement".into();
    replacement.payload["commandId"] = json!("replacement");
    p.store.prepare(&p.scope, &replacement).await.unwrap();
    p.f.release.notify_one();
    assert!(work.await.unwrap().is_err());
    let pending = p.store.pending(&p.scope).await.unwrap().unwrap();
    assert_eq!(pending.command, replacement);
    assert!(pending.response.is_none());
}

#[tokio::test]
async fn changing_actor_during_observation_does_not_settle_the_old_actor() {
    let p = Pending::new().await;
    p.f.hold.store(true, Ordering::SeqCst);
    let api = p.f.api.clone();
    let catalog = p.catalog();
    let work = tokio::spawn(async move {
        api.page_command(PageCommandCall {
            plugin: "example.poll",
            catalog: &catalog,
            viewer: "u1",
            operation: QUERY,
            payload: json!({}),
            conversation: None,
            policy: &Pending::policy(),
            continuation: None,
        })
        .await
    });
    p.f.entered.notified().await;
    *p.f.user.write().await = "other-user".into();
    p.f.release.notify_one();
    assert!(work.await.unwrap().is_err());
    p.assert_unknown().await;
}
