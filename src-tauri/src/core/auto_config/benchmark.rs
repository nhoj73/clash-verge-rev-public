//! Latency measurement of proxy nodes through the running core.
//!
//! `Mihomo::delay_proxy_by_name` reports an unreachable node as `Ok(ProxyDelay { delay: 0 })`
//! rather than as an error, so a zero delay means "dead" and never "instant". Only nodes that
//! answer with a non-zero delay leave this module, which keeps that trap from reaching the
//! selection step: picking the lowest delay without this filter would elect the dead nodes.
//!
//! The core can only measure nodes in the config it currently has loaded, so the caller decides
//! which names to pass in and when.

use crate::config::Config;
use crate::core::handle::Handle;
use clash_verge_logging::{Type, logging};
use futures::{StreamExt as _, stream};
use serde_yaml_ng::Value;

/// The tray tests at this concurrency, so a benchmark does not out-pressure the core.
const DELAY_TEST_CONCURRENCY: usize = 10;
const DEFAULT_TEST_URL: &str = "http://www.gstatic.com/generate_204";
const DEFAULT_TIMEOUT_MS: i16 = 5000;

/// A node that answered the delay test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDelay {
    pub name: std::string::String,
    pub delay_ms: u32,
}

/// Measures every name against the running core and returns only the nodes that answered.
///
/// `names` is owned and moved into the stream, the way the tray's latency test does it: a stream
/// borrowing a slice makes the buffered future's `Send` bound higher-ranked, which cannot cross the
/// agent task's spawn boundary.
pub async fn measure_nodes(names: Vec<std::string::String>) -> Vec<NodeDelay> {
    let (test_url, timeout_ms) = test_settings().await;
    let mihomo = Handle::mihomo();
    let requested = names.len();

    let nodes: Vec<NodeDelay> = stream::iter(names.into_iter().map(|name| {
        let test_url = test_url.clone();
        async move {
            match mihomo.delay_proxy_by_name(&name, &test_url, timeout_ms).await {
                Ok(delay) => answered(&name, delay.delay),
                Err(error) => {
                    logging!(debug, Type::Network, "delay test for {name} failed: {error}");
                    None
                }
            }
        }
    }))
    .buffer_unordered(DELAY_TEST_CONCURRENCY)
    .filter_map(std::future::ready)
    .collect()
    .await;

    logging!(
        info,
        Type::Network,
        "delay test: {} of {} nodes answered",
        nodes.len(),
        requested
    );
    nodes
}

/// The lowest delay among the nodes that answered.
pub fn fastest(nodes: &[NodeDelay]) -> Option<&NodeDelay> {
    nodes.iter().min_by_key(|node| node.delay_ms)
}

/// Inline node names from a candidate config. A provider-based config carries no inline names and
/// yields an empty list, because its nodes only exist once the core has fetched the provider.
pub fn node_names(yaml: &str) -> Vec<std::string::String> {
    let Ok(Value::Mapping(config)) = serde_yaml_ng::from_str::<Value>(yaml) else {
        return Vec::new();
    };
    let Some(Value::Sequence(items)) = config.get(Value::from("proxies")) else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|entry| match entry {
            Value::Mapping(map) => map.get(Value::from("name")).and_then(Value::as_str),
            _ => None,
        })
        .map(str::to_owned)
        .collect()
}

/// Reuses the latency settings the rest of the app already honours, falling back to the values the
/// agent was specified with.
async fn test_settings() -> (std::string::String, u32) {
    let verge = Config::verge().await.latest_arc();
    let test_url = verge
        .default_latency_test
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .unwrap_or(DEFAULT_TEST_URL)
        .to_owned();
    let timeout_ms = verge
        .default_latency_timeout
        .filter(|timeout| *timeout > 0)
        .unwrap_or(DEFAULT_TIMEOUT_MS) as u32;

    (test_url, timeout_ms)
}

/// A zero delay is the core's way of reporting a timeout, so the node is dropped instead of being
/// allowed into the comparison.
fn answered(name: &str, delay_ms: u32) -> Option<NodeDelay> {
    if delay_ms == 0 {
        logging!(debug, Type::Network, "node {name} did not answer within the timeout");
        return None;
    }

    Some(NodeDelay {
        name: name.to_owned(),
        delay_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::{NodeDelay, answered, fastest, node_names};

    fn node(name: &str, delay_ms: u32) -> NodeDelay {
        NodeDelay {
            name: name.to_owned(),
            delay_ms,
        }
    }

    #[test]
    fn a_zero_delay_is_an_unreachable_node_not_a_winner() {
        assert_eq!(answered("dead", 0), None);
        assert_eq!(answered("alive", 42), Some(node("alive", 42)));
    }

    #[test]
    fn fastest_picks_the_lowest_answered_delay() {
        let nodes = vec![node("slow", 300), node("fast", 40), node("mid", 120)];
        assert_eq!(fastest(&nodes), Some(&node("fast", 40)));
        assert_eq!(fastest(&[]), None);
    }

    #[test]
    fn node_names_reads_inline_proxies_only() {
        let yaml = "proxies:\n  - name: a\n    type: ss\n  - name: b\n    type: vmess\n";
        assert_eq!(node_names(yaml), vec!["a".to_owned(), "b".to_owned()]);
        assert!(node_names("proxy-providers:\n  airport:\n    type: http\n").is_empty());
        assert!(node_names("proxies: [unterminated").is_empty());
    }
}
