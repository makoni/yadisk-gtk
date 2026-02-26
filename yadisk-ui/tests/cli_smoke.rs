use std::process::Command;

use serde_json::Value;

fn run_ui(args: &[&str]) -> std::process::Output {
    let exe = env!("CARGO_BIN_EXE_yadisk-ui");
    Command::new(exe)
        .args(args)
        .output()
        .expect("yadisk-ui should execute")
}

#[test]
fn help_lists_primary_commands() {
    let output = run_ui(&["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--start-auth"));
    assert!(stdout.contains("--check-integrations"));
    assert!(stdout.contains("--diagnostics"));
}

#[test]
fn show_settings_outputs_json() {
    let output = run_ui(&["--show-settings"]);
    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert!(json.get("sync_root").is_some());
    assert!(json.get("cache_root").is_some());
    assert!(json.get("autostart").is_some());
}

#[test]
fn diagnostics_outputs_json() {
    let output = run_ui(&["--diagnostics"]);
    assert!(output.status.success());
    let json: Value = serde_json::from_slice(&output.stdout).expect("valid json");
    assert!(json.get("service_state").is_some());
    assert!(json.get("integrations").is_some());
    assert!(json.get("settings").is_some());
}
