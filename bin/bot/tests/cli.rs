//! End-to-end tests that run the real binary as a child process.
//!
//! These are the only tests that exercise what an operator actually does:
//! a process, a config file, environment variables, log output, and a signal.
//! Everything below the binary is unit-tested in its own crate.
//!
//! The child's environment is cleared and rebuilt explicitly, so a stray
//! `ALLOW_PRODUCTION` or `RUST_LOG` in the developer's shell cannot change what
//! these tests prove.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// Canary values placed in the credential env vars. They must never appear in
/// any log output.
const CANARY_KEY: &str = "CANARY-API-KEY-MUST-NOT-BE-LOGGED";
const CANARY_SECRET: &str = "CANARY-API-SECRET-MUST-NOT-BE-LOGGED";

const TESTNET_CONFIG: &str = r#"
environment = "testnet"

[binance]
spot_rest_url  = "https://testnet.binance.vision"
spot_ws_url    = "wss://stream.testnet.binance.vision/ws"
recv_window_ms = 5000

[logging]
level = "debug"
json  = false
"#;

fn production_config() -> String {
    TESTNET_CONFIG.replace(r#""testnet""#, r#""production""#)
}

/// A prepared run: a temp directory holding the config and the captured log.
struct Run {
    _dir: TempDir,
    config: PathBuf,
    log: PathBuf,
    cmd: Command,
}

impl Run {
    fn new(config_body: Option<&str>) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let config = dir.path().join("config.toml");
        if let Some(body) = config_body {
            std::fs::write(&config, body).expect("write config");
        }
        let log = dir.path().join("bot.log");
        let cmd = base_command(&config, &log);
        Self {
            _dir: dir,
            config,
            log,
            cmd,
        }
    }

    /// A run pointed at a config path that does not exist.
    fn missing_config() -> Self {
        let mut run = Self::new(None);
        run.config = PathBuf::from("/nonexistent/config.toml");
        run.cmd = base_command(&run.config, &run.log);
        run
    }

    fn env(mut self, key: &str, value: &str) -> Self {
        self.cmd.env(key, value);
        self
    }

    fn without(mut self, key: &str) -> Self {
        self.cmd.env_remove(key);
        self
    }

    fn read_log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Run to completion. For cases expected to exit on their own.
    fn run_to_completion(mut self) -> Output {
        let status = self.cmd.status().expect("spawn bot");
        Output {
            code: status.code(),
            log: self.read_log(),
        }
    }

    /// Spawn, wait for `marker` to appear in the log, then send SIGINT and reap.
    ///
    /// Polls the log file rather than sleeping a fixed duration, so it is fast on
    /// a fast machine and still correct on a slow one.
    fn run_until_marker_then_interrupt(mut self, marker: &str) -> Output {
        let mut child: Child = self.cmd.spawn().expect("spawn bot");

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = child.try_wait().expect("poll child") {
                panic!(
                    "bot exited early with {status:?} before `{marker}`:\n{}",
                    self.read_log()
                );
            }
            if self.read_log().contains(marker) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "`{marker}` never appeared within 20s:\n{}",
                self.read_log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        interrupt(&child);
        let status = child.wait().expect("reap bot");
        Output {
            code: status.code(),
            log: self.read_log(),
        }
    }
}

/// Build a command for the real binary with a deliberately minimal environment,
/// sending both stdout and stderr to `log`.
fn base_command(config: &Path, log: &Path) -> Command {
    let out = File::create(log).expect("create log file");
    let err = out.try_clone().expect("clone log handle");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_bot"));
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("BINANCE_API_KEY", CANARY_KEY)
        .env("BINANCE_API_SECRET", CANARY_SECRET)
        .arg("--config")
        .arg(config)
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err));
    cmd
}

fn interrupt(child: &Child) {
    let status = Command::new("/bin/kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGINT");
    assert!(status.success(), "kill -INT failed");
}

struct Output {
    code: Option<i32>,
    log: String,
}

impl Output {
    fn assert_contains(&self, needle: &str, why: &str) {
        assert!(
            self.log.contains(needle),
            "{why} (missing `{needle}`):\n{}",
            self.log
        );
    }

    fn assert_no_secrets(&self) {
        assert!(
            !self.log.contains(CANARY_KEY),
            "API key leaked:\n{}",
            self.log
        );
        assert!(
            !self.log.contains(CANARY_SECRET),
            "API secret leaked:\n{}",
            self.log
        );
    }
}

// --- refusal paths: these exit on their own, no signal needed ---

#[test]
fn production_without_the_confirmation_refuses_to_start() {
    let out = Run::new(Some(&production_config())).run_to_completion();

    assert_eq!(out.code, Some(1), "must exit non-zero:\n{}", out.log);
    out.assert_contains("refusing to start", "must say it is refusing");
    out.assert_contains("ALLOW_PRODUCTION", "must name the variable");
    out.assert_contains(
        "I_UNDERSTAND_THE_RISK",
        "must state the exact required value",
    );
    out.assert_no_secrets();
}

#[test]
fn a_merely_truthy_confirmation_still_refuses_to_start() {
    for sloppy in ["true", "1", "yes", "i_understand_the_risk"] {
        let out = Run::new(Some(&production_config()))
            .env("ALLOW_PRODUCTION", sloppy)
            .run_to_completion();

        assert_eq!(
            out.code,
            Some(1),
            "`{sloppy}` must not arm production:\n{}",
            out.log
        );
        out.assert_contains("refusing to start", "must refuse");
    }
}

#[test]
fn missing_api_key_fails_loudly() {
    let out = Run::new(Some(TESTNET_CONFIG))
        .without("BINANCE_API_KEY")
        .run_to_completion();

    assert_eq!(out.code, Some(1), "{}", out.log);
    out.assert_contains("BINANCE_API_KEY", "must name the missing variable");
    out.assert_contains(".env.example", "should point at the documentation");
}

#[test]
fn missing_api_secret_fails_loudly() {
    let out = Run::new(Some(TESTNET_CONFIG))
        .without("BINANCE_API_SECRET")
        .run_to_completion();

    assert_eq!(out.code, Some(1), "{}", out.log);
    out.assert_contains("BINANCE_API_SECRET", "must name the missing variable");
}

#[test]
fn a_missing_config_file_fails_loudly_and_names_the_path() {
    let out = Run::missing_config().run_to_completion();

    assert_eq!(out.code, Some(1), "{}", out.log);
    out.assert_contains(
        "/nonexistent/config.toml",
        "must name the path it looked for",
    );
}

#[test]
fn an_invalid_config_fails_loudly_rather_than_starting() {
    let out =
        Run::new(Some(&TESTNET_CONFIG.replace("recv_window_ms = 5000", ""))).run_to_completion();

    assert_eq!(out.code, Some(1), "{}", out.log);
    out.assert_contains("recv_window_ms", "must name the offending field");
}

// --- the happy path: boot, log, heartbeat, exit cleanly on Ctrl-C ---

#[test]
fn boots_logs_a_redacted_summary_heartbeats_and_exits_cleanly_on_ctrl_c() {
    let out = Run::new(Some(TESTNET_CONFIG)).run_until_marker_then_interrupt("heartbeat");

    assert_eq!(out.code, Some(0), "Ctrl-C must exit cleanly:\n{}", out.log);

    out.assert_contains("starting", "a startup line");
    out.assert_contains("configuration", "a config summary");
    out.assert_contains("testnet.binance.vision", "the summary shows the endpoint");
    out.assert_contains("recv_window_ms", "the summary shows the recv window");
    out.assert_contains(
        "loaded from environment (redacted)",
        "credentials noted, not shown",
    );
    out.assert_contains("strategy loaded", "the injected strategy is announced");
    out.assert_contains("noop", "and it is the concrete strategy bot chose");
    out.assert_contains("heartbeat", "the idle loop is alive");
    out.assert_contains("shutdown requested", "the signal was received");
    out.assert_contains("engine stopped cleanly", "and the loop exited on purpose");

    out.assert_no_secrets();
    out.assert_contains("<redacted>", "the credentials debug line shows redaction");
}

#[test]
fn production_with_the_exact_confirmation_starts_and_shouts_about_it() {
    let out = Run::new(Some(&production_config()))
        .env("ALLOW_PRODUCTION", "I_UNDERSTAND_THE_RISK")
        .run_until_marker_then_interrupt("heartbeat");

    assert_eq!(out.code, Some(0), "{}", out.log);
    out.assert_contains(
        "PRODUCTION MODE - ORDERS WILL USE REAL FUNDS",
        "the production banner must be impossible to miss",
    );
    out.assert_no_secrets();
}
