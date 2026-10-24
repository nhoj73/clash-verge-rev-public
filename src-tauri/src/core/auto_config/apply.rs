//! Writing a validated candidate as a frozen local profile, reloading the core with it, and
//! rolling the switch back.
//!
//! Two rollback layers because the two failures are different. A reload the core rejects is already
//! undone inside [`patch_profiles_config`], which discards the draft and restores the previous
//! profile; a switch that leaves the probe target unreachable is a config the core *accepted*, so
//! only a second switch undoes it — that is what [`verify_or_revert`] is for.
//!
//! Both locks are taken in the order every other config writer uses, `Config::lock_config_write`
//! then `CoreManager::claim_config_update`, and both are released before the switch, because the
//! switch takes them itself: it serializes against `PROFILE_WRITE_LOCK` and then reaches
//! `Config::sync_dns_override`, so a writer holding the configuration lock across a switch would
//! end up waiting on its own lock forever.
//!
//! [`patch_profiles_config`]: crate::cmd::profile::patch_profiles_config

use super::{
    AgentFailure,
    probe::{ProbeVerdict, probe_connectivity},
};
use crate::cmd::profile::{ProfileCreate, create_profile, patch_profiles_config_by_profile_index};
use crate::config::snapshot::{FileSnapshot, capture_config_files, restore_files};
use crate::config::{Config, IProfiles, PrfItem, PrfOption};
use crate::core::CoreManager;
use crate::core::validate::{ValidationOutcome, ValidationSkipReason};
use crate::utils::dirs::{self, PathBufExec as _};
use clash_verge_draft::Draft;
use clash_verge_logging::{Type, logging};
use smartstring::alias::String;

/// Marks the profiles this agent writes, so a later round can find them again.
const PROFILE_PREFIX: &str = "Auto Config ";

/// A profile the agent wrote, plus what was active before it — the rollback target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    pub uid: String,
    pub previous: Option<String>,
}

/// How a write-and-reload round ended.
#[derive(Debug)]
pub enum Switch {
    /// The core reloaded the candidate; confirm it reaches the probe target with
    /// [`verify_or_revert`].
    Committed(Committed),
    /// Nothing was reloaded, so there is nothing to roll back and the round should end. Covers the
    /// core update gate being busy, a skipped switch, and the write itself failing. Carries the code
    /// the panel shows plus the wording the log keeps.
    Abandoned(AgentFailure, std::string::String),
    /// The reload failed and the previous config was put back. `reverted` reports whether that
    /// second switch confirmed it.
    Failed {
        attempt: Committed,
        reason: std::string::String,
        reverted: bool,
    },
}

/// What the probe after a successful reload concluded.
#[derive(Debug)]
pub enum Verify {
    /// The new config reaches the probe target.
    Verified,
    /// The local network itself is down, so the new config is not known to be wrong and is kept.
    Inconclusive,
    /// The new config could not reach the probe target and the previous one was put back.
    Reverted,
    /// The new config could not reach the probe target and the previous one could not be restored.
    Stuck(std::string::String),
}

/// Writes `yaml` as a frozen `local` profile and reloads the core with it.
pub async fn write_and_switch(source: &str, yaml: &str) -> Switch {
    let (attempt, snapshots) = match stage_profile(source, yaml).await {
        Ok(staged) => staged,
        Err((failure, reason)) => return Switch::Abandoned(failure, reason),
    };

    match switch_to(&attempt.uid).await {
        Reload::Done => Switch::Committed(attempt),
        // A switch that never started must not be answered with a rollback of our own: something
        // else owns the configuration gate, and `patch_profiles_config` already left the previous
        // profile in place.
        Reload::NotAttempted(reason) => Switch::Abandoned(AgentFailure::SwitchBlocked, reason),
        Reload::Failed(reason) => {
            let reverted = restore_previous(&attempt, &snapshots).await;
            Switch::Failed {
                attempt,
                reason,
                reverted,
            }
        }
    }
}

/// Confirms the switch reaches the probe target, and switches back when it does not.
pub async fn verify_or_revert(attempt: &Committed) -> Verify {
    match probe_connectivity().await {
        ProbeVerdict::Reachable => {
            logging!(info, Type::Config, "profile {} reaches the probe target", attempt.uid);
            Verify::Verified
        }
        ProbeVerdict::LocalNetworkDown => {
            logging!(
                warn,
                Type::Config,
                "profile {} cannot be judged: the local network itself is down",
                attempt.uid
            );
            Verify::Inconclusive
        }
        ProbeVerdict::ProxyFailure => {
            logging!(
                warn,
                Type::Config,
                "profile {} was loaded but still cannot reach the probe target",
                attempt.uid
            );
            match revert(attempt).await {
                Ok(true) => Verify::Reverted,
                Ok(false) => Verify::Stuck("there is no previous profile to switch back to".into()),
                Err(reason) => Verify::Stuck(reason),
            }
        }
    }
}

/// Switches back to the profile that was active before `attempt`.
///
/// `Ok(false)` means there was no previous profile to go back to.
pub async fn revert(attempt: &Committed) -> Result<bool, std::string::String> {
    let Some(previous) = attempt.previous.clone() else {
        return Ok(false);
    };

    match switch_to(&previous).await {
        Reload::Done => {
            logging!(info, Type::Config, "switched back to profile {previous}");
            Ok(true)
        }
        Reload::NotAttempted(reason) | Reload::Failed(reason) => {
            logging!(
                error,
                Type::Config,
                "switching back to profile {previous} failed: {reason}"
            );
            Err(reason)
        }
    }
}

/// Whether the core got a chance to reload.
enum Reload {
    Done,
    /// Busy, skipped, or the command itself failed before the core took the config.
    NotAttempted(std::string::String),
    Failed(std::string::String),
}

async fn switch_to(uid: &String) -> Reload {
    match patch_profiles_config_by_profile_index(uid.clone()).await {
        Ok(outcome) => classify(outcome),
        // The command failed after its own rollback attempt, so the profile list is not settled.
        Err(error) => Reload::Failed(error.to_string()),
    }
}

/// `Busy` and `Skipped` mean the core was never handed the config, which is a different outcome
/// from a rejected one: only the latter needs a rollback.
fn classify(outcome: ValidationOutcome) -> Reload {
    match outcome {
        ValidationOutcome::Valid => Reload::Done,
        ValidationOutcome::Busy => Reload::NotAttempted("a configuration switch is already running".into()),
        ValidationOutcome::Skipped { reason } => Reload::NotAttempted(match reason {
            ValidationSkipReason::Exiting => "application is exiting".into(),
            ValidationSkipReason::Debounced => "the switch was debounced".into(),
        }),
        ValidationOutcome::Invalid { message, .. } => Reload::Failed(message.to_string()),
    }
}

/// Stages the profile under the write locks, then releases them for the switch to take.
///
/// The rollback snapshot is captured in the same guarded section: it is the state the files are
/// expected to be back in, so it must not straddle another writer.
async fn stage_profile(
    source: &str,
    yaml: &str,
) -> Result<(Committed, Vec<FileSnapshot>), (AgentFailure, std::string::String)> {
    let config_write = Config::lock_config_write().await;
    // Refusing the claim says another switch owns the gate, which is the same statement a later
    // `ValidationOutcome::Busy` makes, so it must not be reported as a failed write.
    let update = CoreManager::global()
        .claim_config_update(&config_write)
        .map_err(|error| (AgentFailure::SwitchBlocked, format!("{error:#}")))?;

    let staged = async {
        let attempt = append_frozen_profile(source, yaml).await?;
        let snapshots = capture_config_files()
            .await
            .map_err(|error| format!("the configuration files could not be snapshotted: {error:#}"))?;
        Ok((attempt, snapshots))
    }
    .await
    .map_err(|reason| (AgentFailure::SwitchFailed, reason));

    drop(update);
    drop(config_write);
    staged
}

async fn append_frozen_profile(source: &str, yaml: &str) -> Result<Committed, std::string::String> {
    let profiles = Config::profiles().await;
    let before = profiles.data_arc();
    let previous = before.current.clone();
    prune_previous_profiles(&profiles, previous.as_deref()).await;
    let option = inherited_option(
        previous
            .as_ref()
            .and_then(|uid| before.get_item(uid).ok())
            .and_then(|item| item.option.as_ref()),
    );
    let known = local_uids(&profiles);

    create_profile(
        ProfileCreate {
            // `local` freezes the bytes. A `remote` profile would let the subscription timer
            // re-fetch the GitHub source and replace the config that was benchmarked.
            itype: Some("local".into()),
            name: Some(profile_name(source)),
            desc: Some(format!("auto config agent, from {source}").into()),
            url: None,
            option,
        },
        Some(yaml.into()),
    )
    .await
    .map_err(|error| format!("{error:#}"))?;

    let uid = newly_added_uid(&profiles, &known)
        .ok_or_else(|| format!("the profile list did not gain an entry for {source}"))?;
    logging!(info, Type::Config, "wrote {source} as frozen local profile {uid}");
    Ok(Committed { uid, previous })
}

/// Copies only the enhance-chain items from the profile being replaced.
///
/// Without them `PrfItem::from_local` appends brand new merge/script/rules/proxies/groups entries
/// for each written candidate, and the profile list fills with empty helper rows. `update_interval`
/// is deliberately not copied: a frozen file has no source to re-fetch.
fn inherited_option(previous: Option<&PrfOption>) -> Option<PrfOption> {
    let chain = previous?;
    Some(PrfOption {
        merge: chain.merge.clone(),
        script: chain.script.clone(),
        rules: chain.rules.clone(),
        proxies: chain.proxies.clone(),
        groups: chain.groups.clone(),
        ..PrfOption::default()
    })
}

/// Layer one of the rollback: put the configuration files back, then put the core back.
async fn restore_previous(attempt: &Committed, snapshots: &[FileSnapshot]) -> bool {
    // Restoring happens under the write lock so a concurrent transaction's bytes are not
    // overwritten, and the lock is released before the switch, which takes it itself.
    let restored = {
        let _config_write = Config::lock_config_write().await;
        restore_files(snapshots).await
    };
    if let Err(error) = restored {
        logging!(error, Type::Config, "restoring the configuration files failed: {error}");
        return false;
    }
    revert(attempt).await.unwrap_or(false)
}

fn profile_name(source: &str) -> String {
    format!("{PROFILE_PREFIX}{source}").into()
}

/// Removes the profiles earlier rounds wrote, so a long-running agent does not grow
/// `profiles.yaml` without bound. The newest match stays as this round's rollback material; the
/// profile currently in use stays even when it is older, because a rollback needs its target.
///
/// Only the profile entries and their own files are removed. The enhance-chain items in their
/// options are inherited by the profiles that remain, so `plan_delete_item` — which deletes those
/// shared items too — would break the profile this round is about to write.
async fn prune_previous_profiles(profiles: &Draft<IProfiles>, keep: Option<&str>) {
    let stale = stale_profile_uids(&profiles.latest_arc(), keep);
    if stale.is_empty() {
        return;
    }

    let files = profiles
        .with_data_modify(|mut candidate| async move {
            let mut files = Vec::with_capacity(stale.len());
            if let Some(items) = candidate.items.as_mut() {
                let mut kept = Vec::with_capacity(items.len());
                for item in std::mem::take(items) {
                    if item
                        .uid
                        .as_deref()
                        .is_some_and(|uid| stale.iter().any(|stale_uid| stale_uid == uid))
                    {
                        files.extend(item.file);
                    } else {
                        kept.push(item);
                    }
                }
                *items = kept;
            }
            candidate.save_file().await?;
            Ok((candidate, files))
        })
        .await;

    match files {
        Ok(files) => {
            let count = files.len();
            remove_profile_files(files).await;
            logging!(info, Type::Config, "removed {count} earlier auto config profiles");
        }
        Err(error) => logging!(
            warn,
            Type::Config,
            "earlier auto config profiles could not be removed: {error:#}"
        ),
    }
}

fn stale_profile_uids(profiles: &IProfiles, keep: Option<&str>) -> Vec<String> {
    let Some(items) = profiles.items.as_deref() else {
        return Vec::new();
    };
    let mut candidates: Vec<&PrfItem> = Vec::new();
    for item in items {
        if is_auto_config_profile(item) {
            candidates.push(item);
        }
    }
    // Entries are append-ordered, so the last match is the newest and stays.
    candidates.pop();
    candidates
        .into_iter()
        .filter(|item| item.uid.as_deref() != keep)
        .filter_map(|item| item.uid.clone())
        .collect()
}

fn is_auto_config_profile(item: &PrfItem) -> bool {
    item.itype.as_deref() == Some("local")
        && item
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with(PROFILE_PREFIX))
}

async fn remove_profile_files(files: Vec<String>) {
    let Ok(dir) = dirs::app_profiles_dir() else {
        return;
    };
    for file in files {
        let path = dir.join(file.as_str());
        if let Err(error) = path.remove_if_exists().await {
            logging!(
                warn,
                Type::Config,
                "could not remove the stale profile file {}: {error:#}",
                path.display()
            );
        }
    }
}

fn local_uids(profiles: &Draft<IProfiles>) -> Vec<String> {
    profiles
        .latest_arc()
        .items
        .as_deref()
        .map(item_uids)
        .unwrap_or_default()
}

fn item_uids(items: &[PrfItem]) -> Vec<String> {
    items
        .iter()
        .filter(|item| item.itype.as_deref() == Some("local"))
        .filter_map(|item| item.uid.clone())
        .collect()
}

/// The appended profile is the last of the `local` entries the list did not already hold.
///
/// The uid is minted inside `from_local`, so it cannot be known in advance. It is also not the only
/// new entry: the enhance-chain items are appended first, but they carry their own `itype` values,
/// which the filter above leaves out.
fn newly_added_uid(profiles: &Draft<IProfiles>, known: &[String]) -> Option<String> {
    let items = profiles.latest_arc().items.clone()?;
    item_uids(&items).iter().rev().find(|uid| !known.contains(uid)).cloned()
}

#[cfg(test)]
mod tests {
    use super::{Reload, classify, item_uids, stale_profile_uids};
    use crate::config::{IProfiles, PrfItem};
    use crate::core::validate::{ValidationOutcome, ValidationSkipReason};

    fn profile(uid: &str, itype: &str, name: &str) -> PrfItem {
        PrfItem {
            uid: Some(uid.into()),
            itype: Some(itype.into()),
            name: Some(name.into()),
            ..PrfItem::default()
        }
    }

    #[test]
    fn a_switch_that_never_ran_is_not_a_failure_to_undo() {
        assert!(matches!(classify(ValidationOutcome::Valid), Reload::Done));
        assert!(matches!(classify(ValidationOutcome::Busy), Reload::NotAttempted(_)));
        assert!(matches!(
            classify(ValidationOutcome::Skipped {
                reason: ValidationSkipReason::Debounced
            }),
            Reload::NotAttempted(_)
        ));
        assert!(matches!(
            classify(ValidationOutcome::invalid_from_message("the core rejected it")),
            Reload::Failed(_)
        ));
    }

    #[test]
    fn only_local_profiles_are_rollback_candidates() {
        let item = |uid: &str, itype: &str| PrfItem {
            uid: Some(uid.into()),
            itype: Some(itype.into()),
            ..PrfItem::default()
        };
        let items = [
            item("L-known", "local"),
            item("m-helper", "merge"),
            item("L-new", "local"),
        ];
        assert_eq!(
            item_uids(&items),
            vec!["L-known".to_owned(), "L-new".to_owned()],
            "the chain items the write appends must not be mistaken for the candidate"
        );
    }

    #[test]
    fn cleanup_keeps_the_newest_auto_config_profile() {
        let profiles = IProfiles {
            current: Some("L-user".into()),
            items: Some(vec![
                profile("L-oldest", "local", "Auto Config a/b/c"),
                profile("L-user", "local", "My own config"),
                profile("L-newest", "local", "Auto Config d/e/f"),
            ]),
        };
        assert_eq!(
            stale_profile_uids(&profiles, profiles.current.as_deref()),
            vec!["L-oldest".to_owned()],
            "the user's profile must never be removed, and the newest round's profile is the next rollback target"
        );
    }

    #[test]
    fn cleanup_keeps_the_profile_in_use_even_when_it_is_not_the_newest() {
        let profiles = IProfiles {
            current: Some("L-oldest".into()),
            items: Some(vec![
                profile("L-oldest", "local", "Auto Config a/b/c"),
                profile("L-middle", "local", "Auto Config d/e/f"),
                profile("L-newest", "local", "Auto Config g/h/i"),
            ]),
        };
        assert_eq!(
            stale_profile_uids(&profiles, profiles.current.as_deref()),
            vec!["L-middle".to_owned()],
            "a rollback to the profile in use must still find its entry"
        );
    }

    #[test]
    fn cleanup_ignores_profiles_the_agent_did_not_write() {
        let profiles = IProfiles {
            current: Some("L-user".into()),
            items: Some(vec![
                profile("L-user", "local", "My own config"),
                profile("R-user", "remote", "Auto Config, but remote"),
                profile("L-lookalike", "local", "Auto Configurator"),
            ]),
        };
        assert!(stale_profile_uids(&profiles, profiles.current.as_deref()).is_empty());
    }
}
