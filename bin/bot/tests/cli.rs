//! End-to-end tests that run the real binary as a child process.
//!
//! These are the only tests that exercise what an operator actually does:
//! a process, a config file, environment variables, log output, and a signal.
//! Everything below the binary is unit-tested in its own crate.
//!
//! The child's environment is cleared and rebuilt explicitly, so a stray
//! `ALLOW_PRODUCTION` or `RUST_LOG` in the developer's shell cannot change what
//! these tests prove.

mod fake_feed;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fake_feed::{FakeFeed, Mode};
use tempfile::TempDir;

/// Canary values placed in the credential env vars. They must never appear in
/// any log output.
const CANARY_KEY: &str = "CANARY-API-KEY-MUST-NOT-BE-LOGGED";
const CANARY_SECRET: &str = "CANARY-API-SECRET-MUST-NOT-BE-LOGGED";

/// A config that never reaches the market source, because every test using it
/// refuses before that point. The WebSocket URL is the real testnet host and is
/// never connected to - `cargo test` does not touch the network.
const TESTNET_CONFIG: &str = r#"
environment = "testnet"

[binance]
spot_rest_url  = "https://testnet.binance.vision"
spot_ws_url    = "wss://stream.testnet.binance.vision/stream"
recv_window_ms = 5000

[market]
symbols      = ["BTCUSDT"]
streams      = ["book_ticker", "trade"]
staleness_ms = 10000

[recording]
enabled = false
dir     = "recordings"

[logging]
level = "debug"
json  = false
"#;

fn production_config() -> String {
    TESTNET_CONFIG.replace(r#""testnet""#, r#""production""#)
}

/// A config pointed at a local fake feed, with recording on into `recording_dir`.
///
/// This is what the tests that actually run the engine use, so the child process
/// exercises the whole path - subscribe, normalize, record, log, shut down -
/// without a live feed anywhere in it.
fn feed_config(feed: &FakeFeed, recording_dir: &Path) -> String {
    TESTNET_CONFIG
        .replace("wss://stream.testnet.binance.vision/stream", feed.url())
        .replace("enabled = false", "enabled = true")
        .replace(
            r#"dir     = "recordings""#,
            &format!(r#"dir     = "{}""#, recording_dir.display()),
        )
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
    fn run_to_completion(self) -> Output {
        self.run_to_completion_within(Duration::from_secs(30))
    }

    /// Run to completion, failing rather than blocking forever if it does not.
    ///
    /// The deadline is not decoration: the fail-closed seam this suite exists to
    /// prove is "the feed died, so stop" - and the failure mode it guards
    /// against is a process that keeps running instead. A test that simply waits
    /// would hang rather than report that.
    fn run_to_completion_within(mut self, limit: Duration) -> Output {
        let mut child = self.cmd.spawn().expect("spawn bot");
        let deadline = Instant::now() + limit;

        loop {
            if let Some(status) = child.try_wait().expect("poll child") {
                return Output {
                    code: status.code(),
                    log: self.read_log(),
                };
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "bot did not exit within {limit:?} - it hung instead of stopping:\n{}",
                    self.read_log()
                );
            }
            std::thread::sleep(Duration::from_millis(20));
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

// --- the endpoint cross-check, at the level an operator would hit it ---

#[test]
fn a_testnet_config_pointed_at_a_production_host_refuses_to_start() {
    // The dangerous direction, and the milestone-1 backlog item this closes: a
    // config labelled testnet aimed at the live exchange passes the environment
    // guard and would then trade real funds. The refusal is structural - the
    // source cannot be constructed - so no socket is opened either.
    let out = Run::new(Some(&TESTNET_CONFIG.replace(
        "wss://stream.testnet.binance.vision/stream",
        "wss://stream.binance.com:9443/stream",
    )))
    .run_to_completion();

    assert_eq!(out.code, Some(1), "must exit non-zero:\n{}", out.log);
    out.assert_contains("refusing to connect", "must say it is refusing");
    out.assert_contains("stream.binance.com", "must name the offending host");
    out.assert_no_secrets();
}

#[test]
fn production_with_the_exact_confirmation_shouts_and_still_refuses_a_testnet_endpoint() {
    // Both guards, composed. Production is armed and the banner prints - and the
    // testnet URL is *still* refused, because arming production says nothing
    // about where the config actually points.
    let out = Run::new(Some(&production_config()))
        .env("ALLOW_PRODUCTION", "I_UNDERSTAND_THE_RISK")
        .run_to_completion();

    out.assert_contains(
        "PRODUCTION MODE - ORDERS WILL USE REAL FUNDS",
        "the production banner must be impossible to miss",
    );
    assert_eq!(out.code, Some(1), "must exit non-zero:\n{}", out.log);
    out.assert_contains("refusing to connect", "the endpoint mismatch still refuses");
    out.assert_no_secrets();
}

// --- the happy path: boot, connect, log a live feed, record, exit on Ctrl-C ---

#[test]
fn boots_logs_a_live_feed_records_it_and_exits_cleanly_on_ctrl_c() {
    let feed = FakeFeed::start(Mode::Feed);
    let recordings = TempDir::new().expect("temp dir");
    let out = Run::new(Some(&feed_config(&feed, recordings.path())))
        .run_until_marker_then_interrupt("heartbeat");

    assert_eq!(out.code, Some(0), "Ctrl-C must exit cleanly:\n{}", out.log);

    out.assert_contains("starting", "a startup line");
    out.assert_contains("configuration", "a config summary");
    out.assert_contains("BTCUSDT", "the summary shows the configured symbols");
    out.assert_contains("staleness_ms", "the summary shows the staleness bound");
    out.assert_contains(
        "loaded from environment (redacted)",
        "credentials noted, not shown",
    );
    out.assert_contains("strategy loaded", "the injected strategy is announced");
    out.assert_contains("noop", "and it is the concrete strategy bot chose");

    // The market source, wired through bot and logged by the engine.
    out.assert_contains(
        "recording this session",
        "the recording target is announced",
    );
    out.assert_contains("market feed connected", "Connected is logged at info");
    out.assert_contains("book ticker", "book tickers reach the engine");
    out.assert_contains("trade", "trades reach the engine");
    out.assert_contains("64000.10000000", "the decimal is logged exactly as sent");
    out.assert_contains("event_time", "events carry an ingest-derived timestamp");
    assert!(
        !out.log.contains("market data gap"),
        "a leaping order book updateId must not be reported as a gap:\n{}",
        out.log
    );

    out.assert_contains("heartbeat", "the idle loop is alive");
    out.assert_contains("shutdown requested", "the signal was received");
    out.assert_contains("engine stopped cleanly", "and the loop exited on purpose");
    out.assert_contains(
        "session recording closed",
        "the recording is flushed before the process exits",
    );

    out.assert_no_secrets();
    out.assert_contains("<redacted>", "the credentials debug line shows redaction");

    // The recording is real, replay-shaped, and on disk.
    let file = std::fs::read_dir(recordings.path())
        .expect("recording directory")
        .map(|entry| entry.expect("dir entry").path())
        .find(|path| path.extension().is_some_and(|e| e == "ndjson"))
        .unwrap_or_else(|| panic!("no recording was written:\n{}", out.log));
    let body = std::fs::read_to_string(&file).expect("read the recording");
    let mut lines = body.lines();

    let header = lines.next().expect("a header line");
    assert!(
        header.contains(r#""format":"binance-market-ndjson""#),
        "{header}"
    );
    assert!(header.contains(r#""endpoint":"testnet""#), "{header}");
    assert!(header.contains("btcusdt@bookTicker"), "{header}");

    let data: Vec<&str> = lines.collect();
    assert!(!data.is_empty(), "the recording has no data lines");
    for line in &data {
        assert!(
            line.contains(r#""recv_ns":"#),
            "no ingest timestamp: {line}"
        );
        assert!(line.contains(r#""seq":"#), "no ingest sequence: {line}");
    }
    assert!(
        data.iter().any(|l| l.contains("btcusdt@bookTicker")),
        "no book ticker payloads were recorded"
    );
    assert!(
        data.iter().any(|l| l.contains("btcusdt@trade")),
        "no trade payloads were recorded"
    );
}

// --- the fail-closed seam, proven end to end ---

#[test]
fn a_dead_market_source_shuts_the_whole_process_down_and_does_not_hang() {
    // The seam: the source hits something fatal (the exchange refusing our
    // subscription, which will not fix itself), the channel closes, the engine
    // treats a dead feed as critical and stops, and the process exits with the
    // source's own reason rather than sitting there looking healthy.
    let feed = FakeFeed::start(Mode::Refuse);
    let recordings = TempDir::new().expect("temp dir");

    let out = Run::new(Some(&feed_config(&feed, recordings.path())))
        .run_to_completion_within(Duration::from_secs(30));

    assert_eq!(
        out.code,
        Some(1),
        "a dead feed must exit non-zero, not linger:\n{}",
        out.log
    );

    out.assert_contains("market feed connected", "it did connect first");
    out.assert_contains("refused our subscription", "the source says why it gave up");
    out.assert_contains(
        "shutting down rather than running blind",
        "the engine logs the dead feed as critical",
    );
    out.assert_contains(
        "engine stopped cleanly",
        "and stops on purpose, not by crashing",
    );
    out.assert_contains(
        "market source stopped with an error",
        "the real reason surfaces from the source, not invented by the engine",
    );
    out.assert_no_secrets();
}
