//! Composer entries whose plugin says whether they can be used now
//! (`availability` on a composer-action, see
//! [`flare_plugin_protocol::ui::UiEntryAvailability`]).
//!
//! The SDK asks each declared `query` (input `{}`) before listing the entry,
//! concurrently and within [`AVAILABILITY_TIME_LIMIT`]; only a boolean `true`
//! at the declared path lists it. An error, a refusal, a timeout or any other
//! answer hides it. Answers are kept per (user, plugin, operation) for at most
//! [`AVAILABILITY_TTL_MS`], and forgotten when the catalog is invalidated (a
//! catalog notice, [`CapabilityApi::invalidate_plugin_catalog`], a plugin
//! answering disabled or not installed), so opening the "+" panel again does
//! not call the plugin each time. Presentation only: the plugin still refuses
//! an invocation it cannot serve.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use flare_plugin_protocol::ui::UiEntryAvailability;
use serde_json::Value;

use super::CapabilityApi;
use crate::extension::capability::ui::PluginComposerAction;

/// Longest wait for all availability answers of one listing.
pub(crate) const AVAILABILITY_TIME_LIMIT: Duration = Duration::from_millis(2000);
/// How long one answer is reused.
pub(crate) const AVAILABILITY_TTL_MS: i64 = 10_000;

#[derive(Clone)]
struct Answer {
    data: Option<Value>,
    generation: u64,
    at_ms: i64,
}

/// Recent availability answers, keyed by (user, plugin, operation).
#[derive(Default)]
pub(super) struct EntryAvailability {
    answers: Mutex<HashMap<(String, String, String), Answer>>,
}
impl EntryAvailability {
    fn answer(
        &self,
        key: &(String, String, String),
        generation: u64,
        now: i64,
    ) -> Option<Option<Value>> {
        let mut answers = self.answers.lock().unwrap_or_else(PoisonError::into_inner);
        // Answers of another catalog generation or past their lifetime go.
        answers.retain(|_, a| a.generation == generation && now - a.at_ms < AVAILABILITY_TTL_MS);
        answers.get(key).map(|a| a.data.clone())
    }
    fn remember(
        &self,
        key: (String, String, String),
        data: Option<Value>,
        generation: u64,
        now: i64,
    ) {
        self.answers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                key,
                Answer {
                    data,
                    generation,
                    at_ms: now,
                },
            );
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl CapabilityApi {
    /// `actions` without the entries their plugin does not admit now.
    pub(super) async fn admitted_composer_actions(
        &self,
        actions: Vec<PluginComposerAction>,
    ) -> crate::shared::error::Result<Vec<PluginComposerAction>> {
        if actions.iter().all(|a| a.availability.is_none()) {
            return Ok(actions);
        }
        let session = self.session_guard.capture_user().await?;
        let user = session.clone().unwrap_or_default();
        let generation = self.catalog_flight.generation();
        let now = now_ms();
        let mut asks: Vec<(String, UiEntryAvailability)> = Vec::new();
        let mut known: HashMap<(String, String), Option<Value>> = HashMap::new();
        for action in &actions {
            let Some(availability) = &action.availability else {
                continue;
            };
            let pair = (action.plugin_id.clone(), availability.operation.clone());
            if known.contains_key(&pair)
                || asks
                    .iter()
                    .any(|(plugin, a)| *plugin == pair.0 && a.operation == pair.1)
            {
                continue;
            }
            let key = (user.clone(), pair.0.clone(), pair.1.clone());
            match self.entry_availability.answer(&key, generation, now) {
                Some(admitted) => {
                    known.insert(pair, admitted);
                }
                None => asks.push((action.plugin_id.clone(), availability.clone())),
            }
        }
        let answers = futures::future::join_all(asks.iter().map(|(_, availability)| async move {
            match crate::shared::util::time::timeout(
                AVAILABILITY_TIME_LIMIT,
                self.dispatch(
                    &availability.operation,
                    Value::Object(Default::default()),
                    None,
                    None,
                    None,
                ),
            )
            .await
            {
                Ok(Ok(result)) => result.success.then_some(result.data),
                Ok(Err(error)) => {
                    tracing::debug!(target: "flare_sdk.plugin", operation = %availability.operation, %error, "composer entry availability unavailable; entry hidden");
                    None
                }
                Err(_) => {
                    tracing::debug!(target: "flare_sdk.plugin", operation = %availability.operation, "composer entry availability timed out; entry hidden");
                    None
                }
            }
        }))
        .await;
        for ((plugin, availability), admitted) in asks.into_iter().zip(answers) {
            self.entry_availability.remember(
                (user.clone(), plugin.clone(), availability.operation.clone()),
                admitted.clone(),
                generation,
                now_ms(),
            );
            known.insert((plugin, availability.operation), admitted);
        }
        // A session that changed meanwhile answers nothing for the old one.
        self.session_guard
            .ensure_unchanged(session.as_deref())
            .await?;
        Ok(actions
            .into_iter()
            .filter(|action| {
                action.availability.as_ref().is_none_or(|availability| {
                    known
                        .get(&(action.plugin_id.clone(), availability.operation.clone()))
                        .and_then(Option::as_ref)
                        .is_some_and(|data| availability.admits(data))
                })
            })
            .collect())
    }
}
