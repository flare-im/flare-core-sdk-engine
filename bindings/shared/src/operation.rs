use flare_im_core_sdk::serde_json::{Map, Value, json};

use crate::contract::MESSAGE_BUILD_OPS;

#[derive(Debug, Clone)]
pub struct NormalizedOperation {
    pub name: String,
    pub request: Value,
}

pub fn message_build_catalog() -> Value {
    Value::Array(
        MESSAGE_BUILD_OPS
            .iter()
            .map(|entry| {
                json!({
                    "op": entry.op,
                    "method": entry.method,
                    "stability": entry.stability,
                    "source_operation": entry.source_operation
                })
            })
            .collect(),
    )
}

pub fn normalize_operation(operation: &str, request: Value) -> NormalizedOperation {
    if let Some(op) = operation.strip_prefix("message_builder.create_") {
        return NormalizedOperation {
            name: "message.build".to_string(),
            request: with_default_op(request, &format!("create_{op}")),
        };
    }

    if operation == "message_builder.list_catalog" {
        return NormalizedOperation {
            name: "message_builder.list_catalog".to_string(),
            request,
        };
    }

    if operation == "message_builder.dispatch" {
        return NormalizedOperation {
            name: "message.build".to_string(),
            request,
        };
    }

    match operation {
        "sync.conversation" => alias("sync.conversation", request),
        "sync.messages" => alias("sync.messages", request),
        "rich_doc_v2.create_message" => NormalizedOperation {
            name: "message.build".to_string(),
            request: with_default_op(request, "create_rich_doc"),
        },
        "rich_doc_v2.edit_message" => alias("message.edit_rich_doc_by_message_id", request),
        op if op.starts_with("rtc.") => NormalizedOperation {
            name: "capability.dispatch".to_string(),
            request: with_default_field(request, "operation", op),
        },
        _ => alias_dynamic(operation, request),
    }
}

fn alias(name: &'static str, request: Value) -> NormalizedOperation {
    NormalizedOperation {
        name: name.to_string(),
        request,
    }
}

fn alias_dynamic(operation: &str, request: Value) -> NormalizedOperation {
    NormalizedOperation {
        name: operation.to_string(),
        request,
    }
}

fn with_default_op(request: Value, op: &str) -> Value {
    with_default_field(request, "op", op)
}

fn with_default_field(request: Value, key: &str, value: &str) -> Value {
    match request {
        Value::Object(mut object) => {
            object
                .entry(key.to_string())
                .or_insert_with(|| Value::String(value.to_string()));
            Value::Object(object)
        }
        Value::Null => {
            let mut object = Map::new();
            object.insert(key.to_string(), Value::String(value.to_string()));
            Value::Object(object)
        }
        other => json!({ key: value, "payload": other }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_removed_client_aliases_unmapped() {
        let op = normalize_operation("client.login", json!({ "userId": "u1" }));
        assert_eq!(op.name, "client.login");
        assert_eq!(op.request["userId"], "u1");
    }

    #[test]
    fn maps_message_builder_contract_to_dispatch_request() {
        let op = normalize_operation(
            "message_builder.create_quote",
            json!({ "conversationId": "c1" }),
        );
        assert_eq!(op.name, "message.build");
        assert_eq!(op.request["op"], "create_quote");
    }

    #[test]
    fn keeps_message_builder_catalog_route_only() {
        let op = normalize_operation("message_builder.list_catalog", Value::Null);
        assert_eq!(op.name, "message_builder.list_catalog");

        let op = normalize_operation("message_builder.create_text", Value::Null);
        assert_eq!(op.name, "message.build");
        assert_eq!(op.request["op"], "create_text");
    }

    #[test]
    fn maps_message_builder_typed_dispatch_to_message_build() {
        let op = normalize_operation(
            "message_builder.dispatch",
            json!({ "op": "create_text", "conversationId": "c1", "text": "hello" }),
        );
        assert_eq!(op.name, "message.build");
        assert_eq!(op.request["op"], "create_text");
        assert_eq!(op.request["conversationId"], "c1");
        assert_eq!(op.request["text"], "hello");
    }

    #[test]
    fn maps_sync_contract_ids_to_direct_routes() {
        let op = normalize_operation("sync.messages", json!({ "conversationId": "c1" }));
        assert_eq!(op.name, "sync.messages");
        let op = normalize_operation("connection.get_status", Value::Null);
        assert_eq!(op.name, "connection.get_status");
    }

    #[test]
    fn maps_create_text_direct_to_direct_message_build_route() {
        let op = normalize_operation(
            "message.create_text",
            json!({ "conversationId": "c1", "text": "hello" }),
        );
        assert_eq!(op.name, "message.create_text");
        assert!(crate::generated::direct_invoke::is_direct_invoke_route(
            &op.name
        ));
    }

    #[test]
    fn does_not_normalize_removed_plural_aliases() {
        let op = normalize_operation("messages.send", json!({ "clientMsgId": "c1" }));
        assert_eq!(op.name, "messages.send");

        let op = normalize_operation("conversations.list", Value::Null);
        assert_eq!(op.name, "conversations.list");

        let op = normalize_operation("events.subscribe", Value::Null);
        assert_eq!(op.name, "events.subscribe");
    }
}
