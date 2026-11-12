use super::*;
use serde_json::json;

fn catalog() -> UiCatalog {
    serde_json::from_value(json!({
        "tenantId":"tenant", "version":1, "validUntilMs":now_ms()+60000,
        "plugins":[{"pluginId":"example.demo", "version":"1.0.0", "manifestDigest":"digest",
            "grantRevision":1, "operations":["example.demo.resolve"],
            "queryOperations":["example.demo.resolve"],
            "extensions":{"platforms":["ios"], "contributions":[
                {"kind":"page","id":"example.demo.page","schemaVersion":1,
                    "requiredPermissions":[],"schema":{"title":"Page"}},
                {"kind":"scan-handler","id":"example.demo.scan","schemaVersion":1,
                    "requiredPermissions":[], "schema":{"title":"Demo",
                        "translations":{"zh-CN":{"title":"示例"}}},
                    "scan":{"routeKey":"demo","resolve":"example.demo.resolve",
                        "pages":["example.demo.page"],"sources":["camera"]}}
            ]}}
        ]
    }))
    .unwrap()
}

#[test]
fn approved_scan_handlers_are_localized_and_serialized_at_the_core_boundary() {
    let handlers = scan_handlers("tenant", &catalog(), ClientPlatform::Ios, "zh-CN");
    assert_eq!(handlers.len(), 1);
    let value = serde_json::to_value(&handlers[0]).unwrap();
    assert_eq!(value["providerName"], "示例");
    assert_eq!(value["routeKey"], "demo");
    assert_eq!(value["sources"], json!(["camera"]));
    assert_eq!(value["pages"], json!(["example.demo.page"]));
}

#[test]
fn tenant_expiry_platform_query_and_page_grants_fail_closed() {
    assert!(scan_handlers("other", &catalog(), ClientPlatform::Ios, "en").is_empty());
    let mut expired = catalog();
    expired.valid_until_ms = now_ms();
    assert!(scan_handlers("tenant", &expired, ClientPlatform::Ios, "en").is_empty());
    assert!(scan_handlers("tenant", &catalog(), ClientPlatform::Android, "en").is_empty());
    let mut no_query = catalog();
    no_query.plugins[0].query_operations.clear();
    assert!(scan_handlers("tenant", &no_query, ClientPlatform::Ios, "en").is_empty());
    let mut no_page = catalog();
    no_page.plugins[0].extensions.contributions.remove(0);
    assert!(scan_handlers("tenant", &no_page, ClientPlatform::Ios, "en").is_empty());
    let mut duplicate = catalog();
    duplicate.plugins.push(duplicate.plugins[0].clone());
    assert!(scan_handlers("tenant", &duplicate, ClientPlatform::Ios, "en").is_empty());
}
