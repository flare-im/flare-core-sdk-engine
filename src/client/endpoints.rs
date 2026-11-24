//! Host-selected addresses for one Core deployment.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoreEndpoints {
    /// Core API base URL, including any deployment path prefix.
    pub base_url: String,
    /// Fully qualified WebSocket gateway URL.
    pub websocket_url: String,
    /// Optional native QUIC gateway; WebSocket remains available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_url: Option<String>,
    /// Optional media API base. Defaults to the Core API base.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_base_url: Option<String>,
}

impl CoreEndpoints {
    pub(crate) fn validate(mut self, allow_cleartext: bool) -> Result<Self> {
        let http: &[_] = if allow_cleartext {
            &["https", "http"]
        } else {
            &["https"]
        };
        let ws: &[_] = if allow_cleartext {
            &["wss", "ws"]
        } else {
            &["wss"]
        };
        self.base_url = endpoint("baseUrl", &self.base_url, http)?;
        self.websocket_url = endpoint("websocketUrl", &self.websocket_url, ws)?;
        self.quic_url = self
            .quic_url
            .map(|url| endpoint("quicUrl", &url, &["quic"]))
            .transpose()?;
        self.media_base_url = self
            .media_base_url
            .map(|url| endpoint("mediaBaseUrl", &url, http))
            .transpose()?;
        // An explicit media base equal to the default describes the same route.
        if self.media_base_url.as_ref() == Some(&self.base_url) {
            self.media_base_url = None;
        }
        Ok(self)
    }

    pub(crate) fn transport_changed(&self, next: &Self) -> bool {
        self.websocket_url != next.websocket_url || self.quic_url != next.quic_url
    }
}

fn endpoint(field: &str, value: &str, schemes: &[&str]) -> Result<String> {
    let error = || {
        FlareError::localized(
            ErrorCode::ConfigurationError,
            format!(
                "endpoints.{field} requires a {} URL without credentials, query or fragment",
                schemes.join(" or ")
            ),
        )
    };
    let parsed = flare_access_contract::ParsedUrl::parse(value.trim()).map_err(|_| error())?;
    if !schemes.contains(&parsed.scheme())
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || (parsed.scheme() == "quic" && !matches!(parsed.path(), "" | "/"))
    {
        return Err(error());
    }
    let canonical = parsed.to_string();
    Ok(if schemes.contains(&"https") {
        canonical.trim_end_matches('/').to_owned()
    } else {
        canonical
    })
}

#[derive(Debug, Clone)]
pub(crate) struct EndpointSnapshot {
    pub revision: u32,
    pub endpoints: CoreEndpoints,
}

pub(crate) struct EndpointStore {
    current: Mutex<Arc<EndpointSnapshot>>,
    pub allow_cleartext: bool,
    // The original deployment identity scopes persistence across address changes.
    pub deployment: String,
}

impl EndpointStore {
    pub fn new(endpoints: CoreEndpoints, allow_cleartext: bool) -> Self {
        Self {
            deployment: endpoints.base_url.clone(),
            current: Mutex::new(Arc::new(EndpointSnapshot {
                revision: 1,
                endpoints,
            })),
            allow_cleartext,
        }
    }

    pub fn snapshot(&self) -> Arc<EndpointSnapshot> {
        self.current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn replace(&self, endpoints: CoreEndpoints) -> Result<EndpointUpdate> {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = current.endpoints != endpoints;
        if changed {
            *current = Arc::new(EndpointSnapshot {
                revision: current.revision.checked_add(1).ok_or_else(|| {
                    FlareError::localized(
                        ErrorCode::ConfigurationError,
                        "the Core endpoint revision is exhausted",
                    )
                })?,
                endpoints,
            });
        }
        Ok(EndpointUpdate {
            revision: current.revision,
            changed,
        })
    }
}

/// Result of accepting a new set of addresses. Link readiness is reported
/// separately through normal connection status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EndpointUpdate {
    /// Accepted endpoint generation for this initialized client.
    pub revision: u32,
    pub changed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addresses() -> CoreEndpoints {
        CoreEndpoints {
            base_url: "https://IM.example.org:443/".into(),
            websocket_url: "wss://IM.example.org:443/gateway".into(),
            quic_url: Some("quic://IM.example.org:8443".into()),
            media_base_url: None,
        }
    }

    #[test]
    fn validates_complete_group_and_rejects_ambiguous_urls() {
        let valid = addresses().validate(false).unwrap();
        assert_eq!(valid.base_url, "https://im.example.org");
        for invalid in [
            "http://im.example.org",
            "https://user@im.example.org",
            "https://im.example.org/#x",
        ] {
            assert!(
                CoreEndpoints {
                    base_url: invalid.into(),
                    ..addresses()
                }
                .validate(false)
                .is_err()
            );
        }
        assert!(
            CoreEndpoints {
                websocket_url: "".into(),
                ..addresses()
            }
            .validate(true)
            .is_err()
        );
    }

    #[test]
    fn replaces_an_immutable_group_as_one_revision() {
        let endpoints = addresses().validate(false).unwrap();
        let store = EndpointStore::new(endpoints.clone(), false);
        let in_flight = store.snapshot();
        assert_eq!(
            store.replace(endpoints.clone()).unwrap(),
            EndpointUpdate {
                revision: 1,
                changed: false
            }
        );
        let next = CoreEndpoints {
            quic_url: None,
            ..endpoints
        };
        assert_eq!(
            store.replace(next).unwrap(),
            EndpointUpdate {
                revision: 2,
                changed: true
            }
        );
        assert!(store.snapshot().endpoints.quic_url.is_none());
        assert!(in_flight.endpoints.quic_url.is_some());
    }
}
