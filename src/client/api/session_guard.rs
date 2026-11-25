//! Session-bound operation guard for SDK facade APIs.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::client::im_client::SessionEndWatch;
use crate::shared::error::{ErrorCode, FlareError, Result};

const SESSION_CANCEL_POLL_MS: u64 = 100;

#[derive(Clone, Default)]
pub(crate) struct SessionGuard {
    current_user_id: Option<Arc<RwLock<String>>>,
    operation_label: &'static str,
    generation: Option<SessionEndWatch>,
}

impl SessionGuard {
    pub(crate) fn disabled(operation_label: &'static str) -> Self {
        Self {
            current_user_id: None,
            operation_label,
            generation: None,
        }
    }

    pub(crate) fn new(current_user_id: Arc<RwLock<String>>, operation_label: &'static str) -> Self {
        Self {
            current_user_id: Some(current_user_id),
            operation_label,
            generation: None,
        }
    }

    pub(crate) fn bind(&mut self, watch: SessionEndWatch) {
        self.generation = Some(watch);
    }

    pub(crate) fn guard_callback<T: 'static>(
        &self,
        callback: Option<Arc<dyn Fn(T) + Send + Sync>>,
    ) -> Option<Arc<dyn Fn(T) + Send + Sync>> {
        let watch = self.generation.clone();
        callback.map(|callback| {
            Arc::new(move |value| {
                if watch
                    .as_ref()
                    .is_none_or(|generation| !generation.is_ended())
                {
                    callback(value);
                }
            }) as _
        })
    }

    pub(crate) fn retired(&self) -> bool {
        self.generation
            .as_ref()
            .is_some_and(SessionEndWatch::is_ended)
    }

    pub(crate) async fn ensure_active(&self) -> Result<()> {
        self.capture_user().await.map(|_| ())
    }

    pub(crate) async fn capture_user(&self) -> Result<Option<String>> {
        if self.retired() {
            return Err(self.session_changed_error());
        }
        let Some(current_user_id) = &self.current_user_id else {
            return Ok(None);
        };
        let user_id = current_user_id.read().await.trim().to_string();
        if self
            .generation
            .as_ref()
            .is_some_and(SessionEndWatch::is_ended)
        {
            return Err(self.session_changed_error());
        }
        if user_id.is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        Ok(Some(user_id))
    }

    pub(crate) async fn ensure_unchanged(&self, expected_user_id: Option<&str>) -> Result<()> {
        if self.retired() {
            return Err(self.session_changed_error());
        }
        let Some(expected_user_id) = expected_user_id else {
            return Ok(());
        };
        let Some(current_user_id) = &self.current_user_id else {
            return Ok(());
        };
        let actual_user_id = current_user_id.read().await.trim().to_string();
        if actual_user_id != expected_user_id
            || self
                .generation
                .as_ref()
                .is_some_and(SessionEndWatch::is_ended)
        {
            return Err(self.session_changed_error());
        }
        Ok(())
    }

    pub(crate) fn session_changed_error(&self) -> FlareError {
        FlareError::localized(
            ErrorCode::NotConnected,
            format!(
                "{} operation cancelled: session changed",
                self.operation_label
            ),
        )
    }

    pub(crate) async fn wait_until_session_changes(&self, expected_user_id: &str) {
        if let Some(watch) = &self.generation {
            return watch.ended().await;
        }
        let Some(current_user_id) = &self.current_user_id else {
            std::future::pending::<()>().await;
            return;
        };
        loop {
            crate::shared::util::delay(Duration::from_millis(SESSION_CANCEL_POLL_MS)).await;
            let actual_user_id = current_user_id.read().await.trim().to_string();
            if actual_user_id != expected_user_id {
                return;
            }
        }
    }

    pub(crate) async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.run_with_user(|_| operation).await
    }

    pub(crate) async fn run_with_user<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(Option<String>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let session_user_id = self.capture_user().await?;
        let operation = operation(session_user_id.clone());
        if session_user_id.is_none() && self.generation.is_none() {
            return operation.await;
        }
        tokio::select! {
            biased;
            _ = self.wait_until_session_changes(session_user_id.as_deref().unwrap_or_default()) => Err(self.session_changed_error()),
            result = operation => {
                self.ensure_unchanged(session_user_id.as_deref()).await?;
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    #[tokio::test]
    async fn run_aborts_when_session_changes_before_operation_completes() {
        let current_user_id = Arc::new(RwLock::new("alice".to_string()));
        let guard = SessionGuard::new(current_user_id.clone(), "test");
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let op_started = started.clone();
        let op_release = release.clone();
        let operation = async move {
            op_started.notify_one();
            op_release.notified().await;
            Ok::<_, FlareError>("stale")
        };

        let task = tokio::spawn(async move { guard.run(operation).await });
        started.notified().await;
        *current_user_id.write().await = String::new();

        let err = timeout(Duration::from_secs(1), task)
            .await
            .expect("session change should abort guard")
            .expect("guard task should not panic")
            .expect_err("session change must fail the operation");
        assert_eq!(err.code(), Some(ErrorCode::NotConnected));
    }
}
