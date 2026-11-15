//! Core 已读摘要与已读名单的 HTTP 接口（api-gateway，Bearer，snake_case，`ApiResponse` 信封）：
//! - `POST /api/v1/messages/read-summaries` `{conversation_id, server_msg_ids}`（1..=50 个互不相同）
//!   → `{conversation_id, available, summaries: [{server_msg_id, read_count, unread_count, total}]}`；
//! - `GET /api/v1/messages/readers?conversation_id&server_msg_id&state=read|unread&cursor&limit`
//!   → `{conversation_id, server_msg_id, available, summary, user_ids, next_cursor, has_more}`。
use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{HttpApiResponse, HttpClient, unwrap_api_response};
use crate::application::services::read_summaries::{
    ReadReceiptMembersPage, ReadReceiptMembersRequest, ReadReceiptTransport, SummaryPage,
};
use crate::model::MessageReadSummary;
use crate::shared::error::Result;

pub(crate) struct HttpReadReceiptTransport {
    http: HttpClient,
}

impl HttpReadReceiptTransport {
    pub(crate) fn new(http: HttpClient) -> Self {
        Self { http }
    }
}

#[derive(Serialize)]
struct SummariesRequest<'a> {
    conversation_id: &'a str,
    server_msg_ids: &'a [String],
}

#[derive(Deserialize)]
struct WireSummary {
    server_msg_id: String,
    read_count: u32,
    unread_count: u32,
    total: u32,
}
impl WireSummary {
    fn summary(&self) -> MessageReadSummary {
        MessageReadSummary {
            read_count: self.read_count,
            unread_count: self.unread_count,
            total: self.total,
        }
    }
}

#[derive(Deserialize)]
struct SummariesData {
    #[serde(default)]
    available: bool,
    #[serde(default)]
    summaries: Vec<WireSummary>,
}

#[derive(Deserialize)]
struct WireReadSummary {
    read_count: u32,
    unread_count: u32,
    total: u32,
}

#[derive(Deserialize)]
struct ReadersData {
    #[serde(default)]
    conversation_id: String,
    #[serde(default)]
    server_msg_id: String,
    #[serde(default)]
    available: bool,
    #[serde(default)]
    summary: Option<WireReadSummary>,
    #[serde(default)]
    user_ids: Vec<String>,
    #[serde(default)]
    next_cursor: String,
    #[serde(default)]
    has_more: bool,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ReadReceiptTransport for HttpReadReceiptTransport {
    async fn summaries(
        &self,
        conversation_id: &str,
        server_msg_ids: &[String],
    ) -> Result<SummaryPage> {
        let body: HttpApiResponse<SummariesData> = self
            .http
            .post(
                "/api/v1/messages/read-summaries",
                &SummariesRequest {
                    conversation_id,
                    server_msg_ids,
                },
            )
            .await?;
        let data = unwrap_api_response(body, "read summaries")?;
        Ok(SummaryPage {
            available: data.available,
            summaries: data
                .summaries
                .iter()
                .map(|s| (s.server_msg_id.clone(), s.summary()))
                .collect(),
        })
    }

    async fn members(&self, request: &ReadReceiptMembersRequest) -> Result<ReadReceiptMembersPage> {
        let mut query = HashMap::from([
            (
                "conversation_id".to_owned(),
                request.conversation_id.clone(),
            ),
            ("server_msg_id".to_owned(), request.server_msg_id.clone()),
            ("state".to_owned(), request.state.wire().to_owned()),
            (
                "cursor".to_owned(),
                request.cursor.clone().unwrap_or_default(),
            ),
        ]);
        if let Some(limit) = request.limit {
            query.insert("limit".to_owned(), limit.to_string());
        }
        let body: HttpApiResponse<ReadersData> = self
            .http
            .get("/api/v1/messages/readers", Some(&query))
            .await?;
        let data = unwrap_api_response(body, "message readers")?;
        Ok(ReadReceiptMembersPage {
            conversation_id: if data.conversation_id.is_empty() {
                request.conversation_id.clone()
            } else {
                data.conversation_id
            },
            server_msg_id: if data.server_msg_id.is_empty() {
                request.server_msg_id.clone()
            } else {
                data.server_msg_id
            },
            available: data.available,
            summary: data
                .summary
                .map(|s| MessageReadSummary {
                    read_count: s.read_count,
                    unread_count: s.unread_count,
                    total: s.total,
                })
                .filter(MessageReadSummary::is_displayable),
            user_ids: data.user_ids,
            next_cursor: (data.has_more && !data.next_cursor.is_empty())
                .then_some(data.next_cursor),
            has_more: data.has_more,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn wire_pages_decode_from_core_shapes() {
        let summaries: HttpApiResponse<SummariesData> =
            crate::shared::util::json::from_value(serde_json::json!({
                "code": 0,
                "data": {"conversation_id": "g1", "available": true, "summaries": [
                    {"server_msg_id": "s1", "read_count": 2, "unread_count": 3, "total": 5}]}
            }))
            .unwrap();
        let data = unwrap_api_response(summaries, "x").unwrap();
        assert!(data.available);
        assert_eq!(data.summaries[0].summary().unread_count, 3);
        let readers: HttpApiResponse<ReadersData> = crate::shared::util::json::from_value(serde_json::json!({
            "code": 0,
            "data": {"conversation_id": "g1", "server_msg_id": "s1", "available": true,
                     "summary": null, "user_ids": ["u1", "u2"], "next_cursor": "", "has_more": false}
        }))
        .unwrap();
        let data = unwrap_api_response(readers, "x").unwrap();
        assert!(data.summary.is_none());
        assert_eq!(data.user_ids.len(), 2);
    }
}
