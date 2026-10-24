//! Connectivity probing for the auto-config agent.
//!
//! The target is probed through the core's mixed port while a domestic control target is probed
//! directly, so a dead local link is never mistaken for a broken proxy path.

use crate::utils::network::{NetworkManager, ProxyType};
use clash_verge_logging::{Type, logging};
use reqwest::StatusCode;

const TARGET_URL: &str = "https://www.youtube.com";
const CONTROL_URL: &str = "https://www.baidu.com";
const PROBE_TIMEOUT_SECS: u64 = 5;

/// What one probe round says about the proxy path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub enum ProbeVerdict {
    /// The target answered through the proxy.
    Reachable,
    /// The control target was unreachable directly, so the local link is down and the proxy
    /// verdict is unknowable.
    LocalNetworkDown,
    /// The control target answered but the target did not: the proxy path is broken.
    ProxyFailure,
}

/// Probes the control target directly and the target through the core's mixed port.
pub async fn probe_connectivity() -> ProbeVerdict {
    let (control_up, target_up) = futures::join!(
        probe_url(CONTROL_URL, ProxyType::None),
        probe_url(TARGET_URL, ProxyType::Localhost),
    );

    match (control_up, target_up) {
        (_, true) => ProbeVerdict::Reachable,
        (false, false) => ProbeVerdict::LocalNetworkDown,
        (true, false) => ProbeVerdict::ProxyFailure,
    }
}

/// A transport error is the expected shape of "unreachable", so it folds into `false` rather than
/// being surfaced: callers only branch on reachability.
async fn probe_url(url: &str, proxy_type: ProxyType) -> bool {
    match NetworkManager::new()
        .head(url, proxy_type, Some(PROBE_TIMEOUT_SECS), None, false)
        .await
    {
        Ok(status) if is_reachable(status) => true,
        Ok(status) => {
            logging!(debug, Type::Network, "connectivity probe {url} answered {status}");
            false
        }
        Err(error) => {
            logging!(debug, Type::Network, "connectivity probe {url} failed: {error:#}");
            false
        }
    }
}

/// Below 400 proves the TCP, TLS and HTTP path works. A blocked proxy typically times out or
/// answers 403; a redirect still means the link is up.
fn is_reachable(status: StatusCode) -> bool {
    status.is_success() || status.is_redirection()
}

/// Counts consecutive proxy failures until the threshold is reached.
///
/// The streak is monitor state, not a retry policy: it spans independent probe rounds and must
/// survive between them, so it deliberately does not use the bounded-retry helpers.
#[derive(Debug)]
pub struct ConnectivityMonitor {
    threshold: u32,
    consecutive_failures: u32,
}

impl ConnectivityMonitor {
    pub const fn new(threshold: u32) -> Self {
        Self {
            threshold,
            consecutive_failures: 0,
        }
    }

    /// Retunes the trigger without disturbing the streak: a user moving the threshold should not
    /// get to watch the agent re-count failures it had already seen.
    pub const fn set_threshold(&mut self, threshold: u32) {
        self.threshold = threshold;
    }

    /// Leaves the streak one failure short of firing again.
    ///
    /// A round that gave up before writing anything did not spend the failures this streak had
    /// already counted, so making the user wait for a whole new streak would leave a dead proxy
    /// unfixed for `threshold` extra intervals.
    pub const fn rearm(&mut self) {
        self.consecutive_failures = self.threshold.saturating_sub(1);
    }

    /// Feeds one verdict in and reports whether the config refresh should start, resetting the
    /// streak whenever it does.
    pub const fn record(&mut self, verdict: ProbeVerdict) -> bool {
        match verdict {
            ProbeVerdict::Reachable => {
                self.consecutive_failures = 0;
                false
            }
            // A dead local link says nothing about the proxy, so the streak is neither grown nor
            // cleared: a streak that predates the outage still counts once the link is back.
            ProbeVerdict::LocalNetworkDown => false,
            ProbeVerdict::ProxyFailure => {
                self.consecutive_failures += 1;
                let reached = self.consecutive_failures >= self.threshold;
                if reached {
                    self.consecutive_failures = 0;
                }
                reached
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectivityMonitor, ProbeVerdict};

    #[test]
    fn triggers_only_after_the_configured_number_of_consecutive_proxy_failures() {
        let mut monitor = ConnectivityMonitor::new(3);

        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(monitor.record(ProbeVerdict::ProxyFailure));
    }

    #[test]
    fn a_reachable_target_clears_a_partially_built_streak() {
        let mut monitor = ConnectivityMonitor::new(3);

        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::Reachable));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(monitor.record(ProbeVerdict::ProxyFailure));
    }

    #[test]
    fn a_dead_local_link_neither_grows_nor_clears_the_streak() {
        let mut monitor = ConnectivityMonitor::new(3);

        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::LocalNetworkDown));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(monitor.record(ProbeVerdict::ProxyFailure));
    }

    #[test]
    fn an_armed_streak_fires_on_one_more_failure_but_reachable_still_resets_it() {
        let mut monitor = ConnectivityMonitor::new(3);

        // The round that fired spent the streak.
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(monitor.record(ProbeVerdict::ProxyFailure));

        monitor.rearm();
        assert!(monitor.record(ProbeVerdict::ProxyFailure));

        monitor.rearm();
        // Re-arming is not a licence to fire on a single failure: a reachable target still wins and
        // puts the full count back.
        assert!(!monitor.record(ProbeVerdict::Reachable));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(!monitor.record(ProbeVerdict::ProxyFailure));
        assert!(monitor.record(ProbeVerdict::ProxyFailure));
    }
}
