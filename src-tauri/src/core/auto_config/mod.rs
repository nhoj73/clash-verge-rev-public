//! Scheduled agent that replaces a dead proxy path with a public config that works.
//!
//! One round runs: probe -> consecutive failures reached the threshold -> GitHub search ->
//! structural and core validation -> write as a frozen local profile and reload the core -> delay
//! test the nodes the core just loaded -> probe again, reverting if the target is still
//! unreachable.
//!
//! The first candidate that survives all of that ends the round. Measuring the alternative — the
//! globally lowest latency across every candidate — costs one core reload per candidate, and each
//! reload drops the live connections, so the round takes the healthy config it found rather than
//! shopping for the fastest one. The fastest node's delay is logged for comparison.
//!
//! The probe runs once more just before each write: a target that recovered while the candidates
//! were being validated ends the round at `Idle` instead of having a working config replaced. Two
//! candidates in a row that load but still cannot reach the target end it too — the rest would only
//! cost more reloads.
//!
//! A round that had to be rolled back or found nothing usable waits for a fresh streak:
//! `ConnectivityMonitor` resets it when the threshold fires, so a failing proxy does not make the
//! agent hammer GitHub on every tick. A round that gave up before touching any configuration is the
//! exception — it never spent the failures it was armed with, so [`ConnectivityMonitor::rearm`]
//! puts the streak back one failure short instead of starting the count over.

mod apply;
mod benchmark;
mod github;
mod probe;
mod validate;

use apply::{Committed, Switch, Verify, revert, verify_or_revert, write_and_switch};
use github::ConfigCandidate;
use probe::{ConnectivityMonitor, ProbeVerdict, probe_connectivity};
use scopeguard::defer;
use serde::Serialize;
use validate::CandidateVerdict;

use crate::config::{Config, IVerge};
use crate::process::AsyncHandler;
use clash_verge_logging::{Type, logging};
use parking_lot::RwLock;
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;

const DEFAULT_INTERVAL_MINUTES: u64 = 30;
const MIN_INTERVAL_MINUTES: u64 = 5;
const MAX_INTERVAL_MINUTES: u64 = 24 * 60;
const DEFAULT_FAILURE_THRESHOLD: u32 = 3;
const MIN_FAILURE_THRESHOLD: u32 = 1;
const MAX_FAILURE_THRESHOLD: u32 = 10;
/// Two candidates that loaded but could not reach the target say the public configs are not
/// getting through; the rest are not worth another core reload.
const MAX_CONSECUTIVE_REVERTS: u32 = 2;

/// What one candidate ended up costing the round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Round {
    /// The config is live and reaches the probe target.
    Adopted,
    /// Rolled back because the core rejected the config; the next candidate may work.
    Next,
    /// The config loaded but could not reach the probe target, and was rolled back.
    Reverted,
    /// Stop touching configuration: the app is exiting, a switch is already owned elsewhere, or a
    /// rollback could not be confirmed.
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Settings {
    enabled: bool,
    interval_minutes: u64,
    failure_threshold: u32,
}

impl Settings {
    fn from_verge(verge: &IVerge) -> Self {
        Self {
            enabled: verge.enable_auto_config_agent.unwrap_or(false),
            interval_minutes: verge
                .auto_config_agent_interval_minutes
                .unwrap_or(DEFAULT_INTERVAL_MINUTES)
                .clamp(MIN_INTERVAL_MINUTES, MAX_INTERVAL_MINUTES),
            failure_threshold: verge
                .auto_config_agent_failure_threshold
                .unwrap_or(DEFAULT_FAILURE_THRESHOLD)
                .clamp(MIN_FAILURE_THRESHOLD, MAX_FAILURE_THRESHOLD),
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_minutes: DEFAULT_INTERVAL_MINUTES,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
        }
    }
}

/// The phase a round is in, as the settings panel shows it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub enum AgentPhase {
    Disabled,
    #[default]
    Idle,
    Probing,
    Searching,
    Validating,
    Switching,
    Benchmarking,
    Verifying,
    Adopted,
    Reverted,
    Abandoned,
    Failed,
}

/// The cause behind a phase that ended without a working config, as one stable code.
///
/// The panel translates this and adds what to do next. The English diagnostic that produced it
/// goes to the log only: it names URLs, uids and core internals, which tells a user nothing they can
/// act on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub enum AgentFailure {
    /// GitHub rejected the configured token.
    TokenInvalid,
    /// GitHub's quota is exhausted; the next round may get through.
    RateLimited,
    /// GitHub could not be reached, or answered something that is not an API reply.
    NetworkBlocked,
    /// Nothing was found to try, or every candidate was rejected.
    NoUsableConfig,
    /// A configuration switch was already owned elsewhere, so nothing was written.
    SwitchBlocked,
    /// The profile write or the core reload failed; the previous config stayed in force.
    SwitchFailed,
    /// The new config loaded and was accepted by the core but still cannot reach the target.
    StillUnreachable,
    /// The local link itself was down when the switch had to be judged.
    LocalNetworkDown,
    /// A switch has to be undone and the rollback did not confirm; needs a manual fix.
    RollbackFailed,
}

/// Everything the settings panel shows without opening the log file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct AgentStatus {
    pub phase: AgentPhase,
    pub enabled: bool,
    pub interval_minutes: u64,
    pub failure_threshold: u32,
    pub round_running: bool,
    pub last_verdict: Option<ProbeVerdict>,
    /// Which candidate the phase is about, as `owner/repo/path`.
    pub source: Option<std::string::String>,
    /// Why the round ended the way it did, when it did not succeed.
    pub failure: Option<AgentFailure>,
}

pub struct AutoConfigAgent {
    settings_tx: watch::Sender<Settings>,
    runner_started: AtomicBool,
    round_running: AtomicBool,
    /// Set by a round that gave up because the app already owned the configuration gate, and read
    /// by the scheduler to arm the probe streak again instead of waiting for a fresh one.
    retry_soon: AtomicBool,
    status: RwLock<AgentStatus>,
}

impl AutoConfigAgent {
    pub fn global() -> &'static Self {
        static INSTANCE: OnceLock<AutoConfigAgent> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let (tx, _rx) = watch::channel(Settings::default());
            Self {
                settings_tx: tx,
                runner_started: AtomicBool::new(false),
                round_running: AtomicBool::new(false),
                retry_soon: AtomicBool::new(false),
                status: RwLock::new(AgentStatus {
                    phase: AgentPhase::default(),
                    enabled: false,
                    interval_minutes: DEFAULT_INTERVAL_MINUTES,
                    failure_threshold: DEFAULT_FAILURE_THRESHOLD,
                    round_running: false,
                    last_verdict: None,
                    source: None,
                    failure: None,
                }),
            }
        })
    }

    /// The panel's live view: the last phase the runner reported, under the settings it is running
    /// with now.
    pub async fn status(&self) -> AgentStatus {
        let settings = Self::load_settings().await;
        let mut status = self.status.read().clone();
        status.enabled = settings.enabled;
        status.interval_minutes = settings.interval_minutes;
        status.failure_threshold = settings.failure_threshold;
        status.round_running = self.round_running.load(Ordering::Acquire);
        // The runner only exists once the switch has been turned on, so a panel opened on a machine
        // that never enabled it would otherwise show the default phase as if it were the agent's.
        if !settings.enabled && !status.round_running {
            status.phase = AgentPhase::Disabled;
        }
        status
    }

    /// Runs one round on demand, for the panel's button. It goes through the same gate as a
    /// scheduled round, and works even while `enable_auto_config_agent` is off.
    pub fn trigger_now(&self) -> Result<(), std::string::String> {
        // A round started now could not finish before the core goes away, and every write it did
        // get through would have to be undone on the way out.
        if crate::core::handle::Handle::global().is_exiting() {
            return Err("the application is exiting".into());
        }
        if self.start_round() {
            Ok(())
        } else {
            Err("a round is already running".into())
        }
    }

    fn start_round(&self) -> bool {
        if self.round_running.swap(true, Ordering::SeqCst) {
            return false;
        }
        let agent = Self::global();
        AsyncHandler::spawn(move || async move {
            defer! {
                agent.round_running.store(false, Ordering::Release);
            }
            run_round().await;
        });
        true
    }

    fn request_retry(&self) {
        self.retry_soon.store(true, Ordering::Release);
    }

    /// Reads and clears the retry request in one step, so it can only pay for one round.
    fn take_retry_hint(&self) -> bool {
        self.retry_soon.swap(false, Ordering::AcqRel)
    }

    fn set_phase(&self, phase: AgentPhase, source: Option<&str>, failure: Option<AgentFailure>) {
        let mut status = self.status.write();
        status.phase = phase;
        status.source = source.map(str::to_owned);
        status.failure = failure;
    }

    fn set_verdict(&self, verdict: ProbeVerdict) {
        self.status.write().last_verdict = Some(verdict);
    }

    /// Reads the cadence at startup.
    pub async fn init(&self) {
        self.refresh_settings().await;
    }

    /// Publishes what `verge.yaml` now says and starts the runner when the switch is on. The runner
    /// is never stopped once started; it idles while `enabled` is false.
    pub async fn refresh_settings(&self) {
        let settings = Self::load_settings().await;
        // Only a real change wakes the runner, so patching an unrelated setting cannot restart the
        // countdown that is already sleeping.
        self.settings_tx.send_if_modified(|current| {
            let changed = *current != settings;
            if changed {
                *current = settings;
            }
            changed
        });
        if settings.enabled {
            self.ensure_runner();
        } else {
            // A retry request only means something to a runner that will tick again.
            self.retry_soon.store(false, Ordering::Release);
        }
    }

    async fn load_settings() -> Settings {
        let verge = Config::verge().await;
        Settings::from_verge(&verge.latest_arc())
    }

    fn ensure_runner(&self) {
        if self.runner_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let rx = self.settings_tx.subscribe();
        AsyncHandler::spawn(move || async move {
            run(rx).await;
        });
    }
}

async fn run(mut rx: watch::Receiver<Settings>) {
    let mut current = *rx.borrow_and_update();
    let mut monitor = ConnectivityMonitor::new(current.failure_threshold);

    loop {
        if !current.enabled {
            report(AgentPhase::Disabled, None, None);
            if rx.changed().await.is_err() {
                return;
            }
            current = adopt(&mut monitor, *rx.borrow_and_update());
            continue;
        }

        let sleeper = tokio::time::sleep(Duration::from_secs(current.interval_minutes.saturating_mul(60)));
        tokio::pin!(sleeper);

        tokio::select! {
            _ = &mut sleeper => {
                // A round reports its own phases; overwriting them with the tick's would make the
                // panel show a probe while the round is actually switching or benchmarking.
                if !AutoConfigAgent::global().round_running.load(Ordering::Acquire) {
                    report(AgentPhase::Probing, None, None);
                }
                let verdict = probe_connectivity().await;
                note_verdict(verdict);
                if monitor.record(verdict) {
                    let threshold = current.failure_threshold;
                    logging!(
                        warn,
                        Type::Core,
                        "auto config agent: {threshold} consecutive {verdict:?} probes, starting a round"
                    );
                    if !AutoConfigAgent::global().start_round() {
                        logging!(debug, Type::Core, "auto config agent: a round is already running");
                    }
                } else {
                    // Read the request every tick so it cannot outlive its reason: only a proxy that
                    // is still broken gets the streak re-armed, and a reachable target leaves the
                    // next round to a full new streak.
                    let retry = AutoConfigAgent::global().take_retry_hint();
                    if retry && verdict == ProbeVerdict::ProxyFailure {
                        logging!(
                            info,
                            Type::Core,
                            "auto config agent: arming the streak again for the round that found the app busy"
                        );
                        monitor.rearm();
                    }
                    if !AutoConfigAgent::global().round_running.load(Ordering::Acquire) {
                        report(AgentPhase::Idle, None, None);
                    }
                }
            }
            changed = rx.changed() => {
                if changed.is_err() {
                    return;
                }
                current = adopt(&mut monitor, *rx.borrow_and_update());
            }
        }
    }
}

/// Moves a changed threshold under the live streak instead of restarting the count.
const fn adopt(monitor: &mut ConnectivityMonitor, settings: Settings) -> Settings {
    monitor.set_threshold(settings.failure_threshold);
    settings
}

fn report(phase: AgentPhase, source: Option<&str>, failure: Option<AgentFailure>) {
    AutoConfigAgent::global().set_phase(phase, source, failure);
}

/// Reports a round that ended without changing anything, and asks the scheduler to arm the probe
/// streak again when the reason was that the app already owned the configuration gate. Waiting a
/// full new streak for a retry would leave a dead proxy unfixed for `threshold` more intervals
/// even though nothing was attempted.
fn report_abandoned(source: Option<&str>, failure: AgentFailure) {
    report(AgentPhase::Abandoned, source, Some(failure));
    if failure == AgentFailure::SwitchBlocked {
        AutoConfigAgent::global().request_retry();
    }
}

fn note_verdict(verdict: ProbeVerdict) {
    AutoConfigAgent::global().set_verdict(verdict);
}

async fn run_round() {
    report(AgentPhase::Searching, None, None);
    let candidates = match github::search_candidates().await {
        Ok(candidates) => candidates,
        Err(error) => {
            logging!(
                warn,
                Type::Network,
                "auto config agent searched no candidates: {error:#}"
            );
            report(AgentPhase::Abandoned, None, Some(github::failure_of(&error)));
            return;
        }
    };
    logging!(
        info,
        Type::Network,
        "auto config agent has {} candidates to try",
        candidates.len()
    );

    let mut consecutive_reverted = 0;
    for candidate in candidates {
        report(AgentPhase::Validating, Some(&candidate.source), None);
        match validate::validate_candidate(&candidate).await {
            CandidateVerdict::Accepted => {}
            CandidateVerdict::Rejected(reason) => {
                logging!(info, Type::Validate, "{} is unusable: {reason}", candidate.source);
                continue;
            }
            // No verdict says something about the app, not about this candidate, so the next one
            // would meet the same gate.
            CandidateVerdict::Inconclusive => {
                report_abandoned(Some(&candidate.source), AgentFailure::SwitchBlocked);
                return;
            }
        }

        // The target can recover while candidates are being validated, and replacing a working
        // config costs the live connections for nothing. Probing again here closes that window.
        let verdict = probe_connectivity().await;
        note_verdict(verdict);
        if verdict == ProbeVerdict::Reachable {
            logging!(
                info,
                Type::Network,
                "auto config agent: the probe target recovered before a switch; leaving the configuration alone"
            );
            report(AgentPhase::Idle, None, None);
            return;
        }

        match try_candidate(candidate).await {
            Round::Adopted => return,
            Round::Next => consecutive_reverted = 0,
            Round::Reverted => {
                consecutive_reverted += 1;
                if consecutive_reverted >= MAX_CONSECUTIVE_REVERTS {
                    logging!(
                        warn,
                        Type::Config,
                        "auto config agent: {consecutive_reverted} candidates in a row loaded but could not reach the target; ending the round"
                    );
                    report(AgentPhase::Idle, None, Some(AgentFailure::NoUsableConfig));
                    return;
                }
            }
            Round::Abort => return,
        }
    }

    report(AgentPhase::Idle, None, Some(AgentFailure::NoUsableConfig));
}

async fn try_candidate(candidate: ConfigCandidate) -> Round {
    let source = candidate.source.as_str();
    report(AgentPhase::Switching, Some(source), None);
    let attempt = match write_and_switch(source, &candidate.content).await {
        Switch::Committed(attempt) => attempt,
        Switch::Abandoned(failure, reason) => {
            logging!(
                warn,
                Type::Config,
                "auto config agent wrote nothing this round: {reason}"
            );
            report_abandoned(Some(source), failure);
            return Round::Abort;
        }
        Switch::Failed {
            attempt,
            reason,
            reverted,
        } => {
            logging!(warn, Type::Config, "applying {} failed: {reason}", candidate.source);
            report(
                AgentPhase::Failed,
                Some(source),
                Some(if reverted {
                    AgentFailure::SwitchFailed
                } else {
                    AgentFailure::RollbackFailed
                }),
            );
            return match reverted {
                true => Round::Next,
                false => {
                    logging!(error, Type::Config, "{} could not be rolled back", attempt.uid);
                    Round::Abort
                }
            };
        }
    };

    let names = benchmark::node_names(&candidate.content);
    if names.is_empty() {
        // A provider-based config has no inline nodes until the core has fetched the provider, so
        // the probe is the only health check it can get.
        logging!(
            info,
            Type::Config,
            "{} declares no inline nodes; relying on the probe alone",
            candidate.source
        );
        return confirm(&attempt, source).await;
    }

    report(AgentPhase::Benchmarking, Some(source), None);
    let measured = benchmark::measure_nodes(names).await;
    match benchmark::fastest(&measured) {
        Some(fastest) => {
            logging!(
                info,
                Type::Config,
                "{}'s fastest node {} answered in {} ms",
                candidate.source,
                fastest.name,
                fastest.delay_ms
            );
            confirm(&attempt, source).await
        }
        None => {
            logging!(
                warn,
                Type::Config,
                "{} has no node that answered the delay test",
                candidate.source
            );
            revert_or_abort(&attempt, source).await
        }
    }
}

async fn confirm(attempt: &Committed, source: &str) -> Round {
    report(AgentPhase::Verifying, Some(source), None);
    match verify_or_revert(attempt).await {
        Verify::Verified => {
            report(AgentPhase::Adopted, Some(source), None);
            Round::Adopted
        }
        Verify::Inconclusive => {
            logging!(
                warn,
                Type::Config,
                "the local link is down; leaving the config as it is"
            );
            report(
                AgentPhase::Abandoned,
                Some(source),
                Some(AgentFailure::LocalNetworkDown),
            );
            Round::Abort
        }
        Verify::Reverted => {
            report(AgentPhase::Reverted, Some(source), Some(AgentFailure::StillUnreachable));
            Round::Reverted
        }
        Verify::Stuck(reason) => {
            logging!(
                error,
                Type::Config,
                "left on a config that cannot reach the target: {reason}"
            );
            report(AgentPhase::Failed, Some(source), Some(AgentFailure::RollbackFailed));
            Round::Abort
        }
    }
}

async fn revert_or_abort(attempt: &Committed, source: &str) -> Round {
    match revert(attempt).await {
        Ok(true) => {
            report(AgentPhase::Reverted, Some(source), Some(AgentFailure::StillUnreachable));
            Round::Reverted
        }
        Ok(false) => {
            logging!(
                error,
                Type::Config,
                "profile {} is live with no earlier profile to go back to",
                attempt.uid
            );
            report(AgentPhase::Failed, Some(source), Some(AgentFailure::RollbackFailed));
            Round::Abort
        }
        Err(reason) => {
            logging!(
                error,
                Type::Config,
                "rolling back profile {} failed: {reason}",
                attempt.uid
            );
            report(AgentPhase::Failed, Some(source), Some(AgentFailure::RollbackFailed));
            Round::Abort
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_FAILURE_THRESHOLD, MAX_INTERVAL_MINUTES, MIN_FAILURE_THRESHOLD, MIN_INTERVAL_MINUTES, Settings};
    use crate::config::IVerge;

    fn settings(mutate: impl FnOnce(&mut IVerge)) -> Settings {
        let mut verge = IVerge::default();
        mutate(&mut verge);
        Settings::from_verge(&verge)
    }

    #[test]
    fn unset_settings_fall_back_to_the_conservative_defaults() {
        assert_eq!(
            Settings::from_verge(&IVerge::default()),
            Settings {
                enabled: false,
                interval_minutes: 30,
                failure_threshold: 3,
            }
        );
    }

    #[test]
    fn a_round_cannot_be_tuned_into_a_probe_storm_or_a_reload_storm() {
        let too_fast = settings(|verge| {
            verge.enable_auto_config_agent = Some(true);
            verge.auto_config_agent_interval_minutes = Some(0);
            verge.auto_config_agent_failure_threshold = Some(0);
        });
        assert_eq!(too_fast.interval_minutes, MIN_INTERVAL_MINUTES);
        assert_eq!(too_fast.failure_threshold, MIN_FAILURE_THRESHOLD);

        let too_slow = settings(|verge| {
            verge.auto_config_agent_interval_minutes = Some(u64::MAX);
            verge.auto_config_agent_failure_threshold = Some(u32::MAX);
        });
        assert_eq!(too_slow.interval_minutes, MAX_INTERVAL_MINUTES);
        assert_eq!(too_slow.failure_threshold, MAX_FAILURE_THRESHOLD);
    }
}
