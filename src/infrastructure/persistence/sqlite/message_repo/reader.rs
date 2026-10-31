use super::*;

#[async_trait]
impl MessageReader for SqliteMessageRepo {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        let row = sqlx::query(&format!(
            "SELECT {} FROM messages WHERE server_id = ?",
            MESSAGE_SELECT_COLS
        ))
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        row.map(|r| self.row_to_immessage(&r)).transpose()
    }

    async fn get_by_message_ids(&self, message_ids: &[String]) -> Result<Vec<IMMessage>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(message_ids.len());
        let mut seen = HashSet::with_capacity(message_ids.len());
        for chunk in message_ids.chunks(super::super::SQLITE_IN_CHUNK) {
            let placeholders = super::super::in_placeholders(chunk.len());
            let sql = format!(
                r#"SELECT {} FROM messages
                   WHERE server_id IN ({})
                   ORDER BY server_id ASC"#,
                MESSAGE_SELECT_COLS, placeholders
            );
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            for row in &rows {
                let message = self.row_to_immessage(row)?;
                if seen.insert(message.server_id.clone()) {
                    out.push(message);
                }
            }
        }
        Ok(out)
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        let row = sqlx::query(&format!(
            r#"SELECT {} FROM messages
               WHERE client_msg_id = ?
               ORDER BY
                 CASE WHEN server_id = client_msg_id THEN 1 ELSE 0 END ASC,
                 conversation_seq DESC,
                 sort_ts DESC,
                 updated_at DESC
               LIMIT 1"#,
            MESSAGE_SELECT_COLS
        ))
        .bind(client_msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        row.map(|r| self.row_to_immessage(&r)).transpose()
    }

    async fn get_by_client_msg_ids(&self, client_msg_ids: &[String]) -> Result<Vec<IMMessage>> {
        if client_msg_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(client_msg_ids.len());
        let mut seen = HashSet::with_capacity(client_msg_ids.len());
        // 单次 `IN (...)` 替代逐条查询；分块以兼容 SQLite 较保守的绑定参数上限。
        for chunk in client_msg_ids.chunks(super::super::SQLITE_IN_CHUNK) {
            let placeholders = super::super::in_placeholders(chunk.len());
            let sql = format!(
                r#"SELECT {} FROM messages
                   WHERE client_msg_id IN ({})
                   ORDER BY
                     client_msg_id ASC,
                     CASE WHEN server_id = client_msg_id THEN 1 ELSE 0 END ASC,
                     conversation_seq DESC,
                     sort_ts DESC,
                     updated_at DESC"#,
                MESSAGE_SELECT_COLS, placeholders
            );
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
            for row in &rows {
                let message = self.row_to_immessage(row)?;
                if seen.insert(message.client_msg_id.clone()) {
                    out.push(message);
                }
            }
        }
        Ok(out)
    }

    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        identity_repair::repair_single_chat_message_alias_for_conversation(
            &self.pool,
            conversation_id,
        )
        .await?;
        // 与 `before_seq_for_sqlite` 一致：`0` / `>= i64::MAX` 表示「最新一页」游标。
        let is_latest_window = before_seq == 0 || before_seq >= i64::MAX as u64;
        let bound = before_seq_for_sqlite(before_seq);
        let cleared_floor = self.local_cleared_floor(conversation_id).await?;

        let rows = if is_latest_window {
            // 本地待 ACK 消息保持在最新窗口顶部；已分配 conversation_seq 的消息以服务端 seq 为权威。
            // 不能再用 max(sort_ts, created_at, client_created_at) 作为主排序，否则设备时钟偏移会让 ACK 后的旧消息长期顶在首屏。
            let sql = if cleared_floor > 0 {
                format!(
                    r#"SELECT {} FROM messages
                   WHERE conversation_id = ? AND conversation_seq < ? AND (conversation_seq = 0 OR conversation_seq > ?)
                   ORDER BY
                     CASE WHEN conversation_seq = 0 AND is_local = 1 THEN 1 ELSE 0 END DESC,
                     CASE WHEN conversation_seq > 0 THEN conversation_seq ELSE 0 END DESC,
                     CASE
                       WHEN conversation_seq > 0 THEN COALESCE(NULLIF(created_at, 0), NULLIF(client_created_at, 0), NULLIF(sort_ts, 0), 0)
                       ELSE max(max(sort_ts, created_at), client_created_at)
                     END DESC
                   LIMIT ?"#,
                    MESSAGE_SELECT_COLS
                )
            } else {
                format!(
                    r#"SELECT {} FROM messages
                   WHERE conversation_id = ? AND conversation_seq < ?
                   ORDER BY
                     CASE WHEN conversation_seq = 0 AND is_local = 1 THEN 1 ELSE 0 END DESC,
                     CASE WHEN conversation_seq > 0 THEN conversation_seq ELSE 0 END DESC,
                     CASE
                       WHEN conversation_seq > 0 THEN COALESCE(NULLIF(created_at, 0), NULLIF(client_created_at, 0), NULLIF(sort_ts, 0), 0)
                       ELSE max(max(sort_ts, created_at), client_created_at)
                     END DESC
                   LIMIT ?"#,
                    MESSAGE_SELECT_COLS
                )
            };
            let mut q = sqlx::query(&sql).bind(conversation_id).bind(bound);
            if cleared_floor > 0 {
                q = q.bind(cleared_floor as i64);
            }
            q.bind(limit as i32).fetch_all(&self.pool).await
        } else {
            // 翻页只拉已分配 conversation_seq 的历史消息，避免 `conversation_seq == 0` 的待发送行在第二页重复出现。
            let sql = if cleared_floor > 0 {
                format!(
                    r#"SELECT {} FROM messages
                   WHERE conversation_id = ? AND conversation_seq > 0 AND conversation_seq < ? AND conversation_seq > ?
                   ORDER BY conversation_seq DESC LIMIT ?"#,
                    MESSAGE_SELECT_COLS
                )
            } else {
                format!(
                    r#"SELECT {} FROM messages
                   WHERE conversation_id = ? AND conversation_seq > 0 AND conversation_seq < ?
                   ORDER BY conversation_seq DESC LIMIT ?"#,
                    MESSAGE_SELECT_COLS
                )
            };
            let mut q = sqlx::query(&sql).bind(conversation_id).bind(bound);
            if cleared_floor > 0 {
                q = q.bind(cleared_floor as i64);
            }
            q.bind(limit as i32).fetch_all(&self.pool).await
        }
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.row_to_immessage(&row)?);
        }
        Ok(out)
    }

    async fn get_by_conversation_after(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        identity_repair::repair_single_chat_message_alias_for_conversation(
            &self.pool,
            conversation_id,
        )
        .await?;
        let cleared_floor = self.local_cleared_floor(conversation_id).await?;
        let low = i64::try_from(after_seq.max(cleared_floor)).unwrap_or(i64::MAX);
        let sql = format!(
            r#"SELECT {} FROM messages
               WHERE conversation_id = ? AND conversation_seq > 0 AND conversation_seq > ?
               ORDER BY conversation_seq ASC LIMIT ?"#,
            MESSAGE_SELECT_COLS
        );
        let rows = sqlx::query(&sql)
            .bind(conversation_id)
            .bind(low)
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.row_to_immessage(&row)?);
        }
        Ok(out)
    }

    async fn oldest_conversation_seq(&self, conversation_id: &str) -> Result<Option<u64>> {
        let row: Option<i64> = sqlx::query_scalar(
            r#"SELECT MIN(conversation_seq) FROM messages
               WHERE conversation_id = ? AND conversation_seq > 0"#,
        )
        .bind(conversation_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        Ok(row
            .and_then(|seq| u64::try_from(seq).ok())
            .filter(|seq| *seq > 0))
    }

    async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.search_by_query(&MessageSearchQuery::text(keyword, limit))
            .await
    }

    async fn search_by_query(&self, query: &MessageSearchQuery) -> Result<Vec<IMMessage>> {
        let effective_time = search_effective_time_sql("messages");
        let mut sql = format!("SELECT {} FROM messages WHERE 1 = 1", MESSAGE_SELECT_COLS);
        sql.push_str(" AND (conversation_seq = 0 OR conversation_seq > COALESCE((SELECT visible_after_seq FROM conversations WHERE conversation_id = messages.conversation_id LIMIT 1), 0))");
        if !query.include_recalled {
            sql.push_str(" AND COALESCE(is_recalled, 0) = 0");
        }

        let mut qb = QueryBuilder::<Sqlite>::new(sql);
        if let Some(conversation_id) = query
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            qb.push(" AND conversation_id = ");
            qb.push_bind(conversation_id);
        }
        if let Some(sender_id) = query
            .sender_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            qb.push(" AND sender_id = ");
            qb.push_bind(sender_id);
        }
        if let Some(keyword) = query.normalized_keyword() {
            let Some(search_plan) = sqlite_keyword_search(&keyword) else {
                return Ok(Vec::new());
            };
            match search_plan {
                SqliteKeywordSearch::Fts(fts_query) => {
                    qb.push(
                        " AND server_id IN (SELECT server_id FROM messages_fts WHERE messages_fts MATCH ",
                    );
                    qb.push_bind(fts_query);
                    qb.push(")");
                }
                SqliteKeywordSearch::ContentLike(like) => {
                    qb.push(" AND (LOWER(COALESCE(text, '')) LIKE ");
                    qb.push_bind(like.clone());
                    qb.push(r#" ESCAPE '\' OR LOWER(COALESCE(CASE WHEN json_valid(attributes) THEN json_extract(attributes, '$.contentText') ELSE '' END, '')) LIKE "#);
                    qb.push_bind(like.clone());
                    qb.push(r#" ESCAPE '\' OR server_id IN (SELECT server_id FROM messages_fts WHERE LOWER(COALESCE(text, '')) LIKE "#);
                    qb.push_bind(like);
                    qb.push(r#" ESCAPE '\'))"#);
                }
            }
        }
        if let Some(from_time) = query.from_time {
            qb.push(" AND ");
            qb.push(&effective_time);
            qb.push(" >= ");
            qb.push_bind(from_time.min(i64::MAX as u64) as i64);
        }
        if let Some(to_time) = query.to_time {
            qb.push(" AND ");
            qb.push(&effective_time);
            qb.push(" <= ");
            qb.push_bind(to_time.min(i64::MAX as u64) as i64);
        }

        let message_types = message_type_values_for_search(&query.kinds);
        if !message_types.is_empty() {
            qb.push(" AND message_type IN (");
            let mut separated = qb.separated(", ");
            for value in message_types {
                separated.push_bind(value);
            }
            separated.push_unseparated(")");
        }

        qb.push(" ORDER BY ");
        qb.push(&effective_time);
        qb.push(" DESC, conversation_seq DESC LIMIT ");
        qb.push_bind(query.normalized_limit() as i32);

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FlareError::localized(ErrorCode::DatabaseError, e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.row_to_immessage(&row)?);
        }
        Ok(out)
    }

    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        let keyword = keyword.trim();
        if keyword.is_empty() {
            return Ok(Vec::new());
        }
        self.search_by_query(&MessageSearchQuery::in_conversation(
            conversation_id,
            keyword,
            limit,
        ))
        .await
    }
}
