use super::{CmdResult, coded_error};
use crate::core::auto_config::{AgentStatus, AutoConfigAgent};

/// What the auto config agent is doing right now, for its settings panel.
#[tauri::command]
#[specta::specta]
pub async fn get_auto_config_status() -> CmdResult<AgentStatus> {
    Ok(AutoConfigAgent::global().status().await)
}

/// Starts one agent round immediately instead of waiting for the probe streak to reach the
/// threshold. The round keeps running after the command returns.
#[tauri::command]
#[specta::specta]
pub async fn run_auto_config_now() -> CmdResult<AgentStatus> {
    AutoConfigAgent::global()
        .trigger_now()
        .map_err(|reason| coded_error("AUTO_CONFIG_ROUND_RUNNING", reason))?;
    Ok(AutoConfigAgent::global().status().await)
}
