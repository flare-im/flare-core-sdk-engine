//! Stable scan-link hosts, signed together with the application's access manifest.
use super::access::{ManifestError, invalid, is_valid_public_host};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinksSection {
    pub primary: String,
    #[serde(default)]
    pub accepted: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family_suffix: Option<String>,
}

impl LinksSection {
    pub fn validate(&self) -> Result<(), ManifestError> {
        if !valid_link_host(&self.primary) || self.accepted.len() > 8 {
            return Err(invalid(
                "links.primary",
                "expected a lowercase DNS host and optional TCP port",
            ));
        }
        let mut hosts = BTreeSet::from([self.primary.as_str()]);
        for host in &self.accepted {
            if !valid_link_host(host) || !hosts.insert(host.as_str()) {
                return Err(invalid(
                    "links.accepted",
                    "hosts must be valid and unique, excluding primary",
                ));
            }
        }
        if self.family_suffix.as_ref().is_some_and(|suffix| {
            suffix != &suffix.to_ascii_lowercase() || !is_valid_public_host(suffix)
        }) {
            return Err(invalid(
                "links.familySuffix",
                "expected a lowercase DNS suffix without a port",
            ));
        }
        Ok(())
    }
}

fn valid_link_host(value: &str) -> bool {
    if value != value.to_ascii_lowercase() {
        return false;
    }
    let host = if let Some((host, port)) = value.rsplit_once(':') {
        if port.parse::<u16>().ok().is_none_or(|port| port == 0)
            || port.starts_with('0')
            || port.len() > 5
        {
            return false;
        }
        host
    } else {
        value
    };
    is_valid_public_host(host)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn link_hosts_are_exact_bounded_and_never_urls() {
        let mut links = LinksSection {
            primary: "scan.example.test:8443".into(),
            accepted: vec!["old.example.test".into()],
            family_suffix: Some("example.test".into()),
        };
        assert!(links.validate().is_ok());
        for bad in [
            "https://scan.example.test",
            "evil.test/path",
            "u@scan.example.test",
            "127.0.0.1",
            "scan.example.test:0",
            "scan.example.test:65536",
            "scan.example.test:0443",
            "scan.example.test.",
            "SCAN.example.test",
            "scan.example.test?x",
        ] {
            links.primary = bad.into();
            assert!(links.validate().is_err(), "{bad}");
        }
        links.primary = "old.example.test".into();
        assert!(links.validate().is_err());
        links.primary = "scan.example.test".into();
        links.family_suffix = Some("example.test:8443".into());
        assert!(links.validate().is_err());
    }
}
