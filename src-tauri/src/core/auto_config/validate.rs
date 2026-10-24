//! Structural and core-level validation of fetched candidate configs.
//!
//! Two layers on purpose: the structural pass is free and rejects files that are not even shaped
//! like a config, so a core subprocess is only spent on candidates that could plausibly load.
//! `CoreConfigValidator` then gives the authoritative verdict by handing the bytes to the real
//! Mihomo binary, which is the same gate `CoreManager` puts every applied config through.

use super::github::ConfigCandidate;
use crate::core::validate::{CoreConfigValidator, ValidationOutcome};
use crate::enhance::field::use_lowercase;
use clash_verge_logging::{Type, logging};
use serde_yaml_ng::{Mapping, Value};
use std::time::Duration;

/// Required on every entry of `proxies`.
const PROXY_FIELDS: [&str; 4] = ["name", "type", "server", "port"];
/// Required on every entry of `proxy-groups`.
const GROUP_FIELDS: [&str; 3] = ["name", "type", "proxies"];
/// The shared validator waits on the core subprocess without a bound, so the agent bounds its own
/// wait instead of changing a gate every other config writer depends on.
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(30);

/// What validation made of one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateVerdict {
    Accepted,
    /// Unusable; the caller should move on to the next candidate.
    Rejected(String),
    /// No verdict was produced, because the validator is busy, debounced, or the app is exiting.
    /// The caller should abandon the whole round rather than try the next candidate, which would
    /// meet the same gate.
    Inconclusive,
}

/// Runs both validation layers over one candidate.
pub async fn validate_candidate(candidate: &ConfigCandidate) -> CandidateVerdict {
    if let Err(reason) = check_structure(&candidate.content) {
        logging!(info, Type::Validate, "rejecting {}: {reason}", candidate.source);
        return CandidateVerdict::Rejected(reason);
    }

    match tokio::time::timeout(
        VALIDATION_TIMEOUT,
        CoreConfigValidator::global().validate_config_outcome_with(&candidate.content),
    )
    .await
    {
        Ok(Ok(outcome)) if outcome.is_valid() => {
            logging!(info, Type::Validate, "core accepted {}", candidate.source);
            CandidateVerdict::Accepted
        }
        Ok(Ok(ValidationOutcome::Invalid { message, .. })) => {
            logging!(info, Type::Validate, "core rejected {}: {message}", candidate.source);
            CandidateVerdict::Rejected(message.to_string())
        }
        Ok(Ok(outcome)) => {
            logging!(
                warn,
                Type::Validate,
                "no verdict for {} this round: {outcome}",
                candidate.source
            );
            CandidateVerdict::Inconclusive
        }
        Ok(Err(error)) => {
            logging!(
                warn,
                Type::Validate,
                "validation of {} could not run: {error:#}",
                candidate.source
            );
            CandidateVerdict::Inconclusive
        }
        Err(_elapsed) => {
            logging!(
                warn,
                Type::Validate,
                "the core took longer than {}s to judge {}; leaving the config alone",
                VALIDATION_TIMEOUT.as_secs(),
                candidate.source
            );
            CandidateVerdict::Inconclusive
        }
    }
}

/// Checks that the three sections the agent depends on are present, are arrays, and carry the
/// fields a node or a group cannot work without.
fn check_structure(yaml: &str) -> Result<(), String> {
    let parsed: Value = serde_yaml_ng::from_str(yaml).map_err(|error| format!("not parseable as YAML: {error}"))?;
    let Value::Mapping(mapping) = parsed else {
        return Err("the top level is not a mapping".to_owned());
    };
    // Top-level keys only, matching the enhance pipeline: the core wants entry fields such as
    // `server` in lower case, so relaxing those would accept configs Mihomo then rejects.
    let config = use_lowercase(&mapping);

    check_node_source(&config)?;
    check_groups(&config)?;
    non_empty_sequence(&config, "rules").map(|_| ())
}

fn check_node_source(config: &Mapping) -> Result<(), String> {
    if let Some(Value::Sequence(items)) = config.get(Value::from("proxies"))
        && !items.is_empty()
    {
        for entry in items {
            entry_mapping(entry, "proxies", &PROXY_FIELDS)?;
        }
        return Ok(());
    }

    // `proxy-providers` is a mapping of provider name to definition, not an array; insisting on
    // inline `proxies` as well would reject every provider-based subscription, which is a common
    // shape for public configs.
    if let Some(Value::Mapping(providers)) = config.get(Value::from("proxy-providers"))
        && !providers.is_empty()
    {
        return Ok(());
    }

    Err("neither a non-empty `proxies` array nor a non-empty `proxy-providers` mapping is present".to_owned())
}

fn check_groups(config: &Mapping) -> Result<(), String> {
    for entry in non_empty_sequence(config, "proxy-groups")? {
        let group = entry_mapping(entry, "proxy-groups", &GROUP_FIELDS)?;
        if !matches!(group.get(Value::from("proxies")), Some(Value::Sequence(_))) {
            return Err("a proxy-groups entry has a `proxies` member that is not an array".to_owned());
        }
    }
    Ok(())
}

fn non_empty_sequence<'a>(config: &'a Mapping, key: &str) -> Result<&'a [Value], String> {
    match config.get(Value::from(key)) {
        Some(Value::Sequence(items)) if !items.is_empty() => Ok(items.as_slice()),
        Some(Value::Sequence(_)) => Err(format!("`{key}` is an empty array")),
        Some(_) => Err(format!("`{key}` is not an array")),
        None => Err(format!("`{key}` is missing")),
    }
}

fn entry_mapping<'a>(entry: &'a Value, section: &str, fields: &[&str]) -> Result<&'a Mapping, String> {
    let Value::Mapping(map) = entry else {
        return Err(format!("a `{section}` entry is not a mapping"));
    };
    for field in fields {
        if map.get(Value::from(*field)).is_none() {
            return Err(format!("a `{section}` entry is missing `{field}`"));
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::check_structure;

    const VALID: &str = r"
proxies:
  - name: node-1
    type: ss
    server: 192.0.2.10
    port: 8388
proxy-groups:
  - name: PROXY
    type: select
    proxies:
      - node-1
rules:
  - GEOIP,CN,DIRECT
";

    #[test]
    fn accepts_a_well_shaped_config() {
        assert_eq!(check_structure(VALID), Ok(()));
    }

    #[test]
    fn top_level_key_case_does_not_matter() {
        let upper = "Proxies:
  - name: node-1
    type: ss
    server: 192.0.2.10
    port: 8388
Proxy-Groups:
  - name: PROXY
    type: select
    proxies: [node-1]
Rules:
  - GEOIP,CN,DIRECT
";
        assert_eq!(check_structure(upper), Ok(()));
    }

    #[test]
    fn rejects_a_missing_or_misshapen_section() {
        let no_nodes = "neither a non-empty `proxies` array nor a non-empty `proxy-providers` mapping is present";
        assert_eq!(
            check_structure("proxies: []\nproxy-groups: []\nrules: []"),
            Err(no_nodes.to_owned())
        );
        assert_eq!(
            check_structure("proxies: 3\nproxy-groups: []\nrules: []"),
            Err(no_nodes.to_owned())
        );
        assert_eq!(check_structure("proxy-groups: []\nrules: []"), Err(no_nodes.to_owned()));
        assert_eq!(
            check_structure("proxies:\n  - name: a\n"),
            Err("a `proxies` entry is missing `type`".to_owned())
        );
        assert_eq!(
            check_structure("proxies:\n  - name: a\n    type: ss\n    server: s\n    port: 1\n"),
            Err("`proxy-groups` is missing".to_owned())
        );
        assert_eq!(check_structure("[]"), Err("the top level is not a mapping".to_owned()));
        assert!(check_structure("proxies: [unterminated").is_err());
    }

    #[test]
    fn a_provider_only_config_is_accepted() {
        let providers = r"
proxy-providers:
  airport:
    type: http
    url: https://example.com/sub.yaml
    interval: 3600
    path: ./airport.yaml
proxy-groups:
  - name: PROXY
    type: select
    proxies:
      - airport
rules:
  - GEOIP,CN,DIRECT
";
        assert_eq!(check_structure(providers), Ok(()));
    }

    #[test]
    fn an_empty_provider_mapping_is_not_a_node_source() {
        assert!(check_structure("proxy-providers: {}\nproxy-groups: []\nrules: []").is_err());
    }

    #[test]
    fn rejects_entries_missing_required_fields() {
        let no_port = r"
proxies:
  - name: node-1
    type: ss
    server: 192.0.2.10
proxy-groups:
  - name: PROXY
    type: select
    proxies: [node-1]
rules:
  - GEOIP,CN,DIRECT
";
        assert_eq!(
            check_structure(no_port),
            Err("a `proxies` entry is missing `port`".to_owned())
        );

        let group_proxies_not_array = r"
proxies:
  - name: node-1
    type: ss
    server: 192.0.2.10
    port: 8388
proxy-groups:
  - name: PROXY
    type: select
    proxies: node-1
rules:
  - GEOIP,CN,DIRECT
";
        assert_eq!(
            check_structure(group_proxies_not_array),
            Err("a proxy-groups entry has a `proxies` member that is not an array".to_owned())
        );
    }
}
