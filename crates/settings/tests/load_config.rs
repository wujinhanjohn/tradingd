//! Integration tests for `settings::load`, exercised through the public API only.

use std::path::{Path, PathBuf};

use settings::{Config, Env, Error};
use tempfile::TempDir;

/// The sample config checked in at the workspace root - the one the bot ships with.
fn repo_config() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("config.toml")
}

fn write_config(body: &str) -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("create temp dir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, body).expect("write config");
    (dir, path)
}

const VALID: &str = r#"
environment = "testnet"

[binance]
spot_rest_url  = "https://testnet.binance.vision"
spot_ws_url    = "wss://stream.testnet.binance.vision/ws"
recv_window_ms = 5000

[logging]
level = "info"
json  = false
"#;

#[test]
fn parses_the_sample_config_shipped_with_the_repo() {
    let config = settings::load(repo_config()).expect("the checked-in config.toml must parse");

    assert_eq!(
        config.environment,
        Env::Testnet,
        "the shipped default must be testnet"
    );
    assert_eq!(
        config.binance.spot_rest_url,
        "https://testnet.binance.vision"
    );
    assert_eq!(
        config.binance.spot_ws_url,
        "wss://stream.testnet.binance.vision/ws"
    );
    assert_eq!(config.binance.recv_window_ms, 5000);
    assert_eq!(config.logging.level, "info");
    assert!(!config.logging.json);
}

#[test]
fn parses_an_equivalent_config_from_a_temp_file() {
    let (_dir, path) = write_config(VALID);
    let config = settings::load(&path).expect("valid config");
    assert_eq!(config.environment, Env::Testnet);
}

#[test]
fn production_parses_but_arming_it_is_a_separate_decision() {
    // `settings` only reports what the file says. Refusing to run is the engine's job.
    let (_dir, path) = write_config(&VALID.replace("testnet\"", "production\""));
    let config = settings::load(&path).expect("production is a valid value");
    assert_eq!(config.environment, Env::Production);
    assert!(config.environment.is_production());
}

#[test]
fn missing_file_names_the_path_it_looked_for() {
    let err = settings::load("definitely/not/here.toml").expect_err("file does not exist");
    assert!(
        matches!(err, Error::ConfigFileNotFound { .. }),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains("definitely/not/here.toml"),
        "{err}"
    );
}

#[test]
fn missing_required_field_is_a_typed_error_not_a_panic() {
    let (_dir, path) = write_config(
        r#"
environment = "testnet"

[binance]
spot_rest_url  = "https://testnet.binance.vision"
spot_ws_url    = "wss://stream.testnet.binance.vision/ws"

[logging]
level = "info"
json  = false
"#,
    );
    let err = settings::load(&path).expect_err("recv_window_ms is absent");
    assert!(matches!(err, Error::InvalidConfig { .. }), "got {err:?}");

    let detail = source_chain(&err);
    assert!(
        detail.contains("recv_window_ms"),
        "must name the field: {detail}"
    );
}

#[test]
fn absent_environment_is_an_error_and_never_falls_through_to_testnet() {
    // `Env` has no `Default` and the field carries no `#[serde(default)]`, so a
    // deployment that forgets to state its environment refuses to start rather
    // than quietly assuming the safe one. "Testnet by default" is the shipped
    // config.toml saying so, not a code-level fallback that can mask a mistake.
    let (_dir, path) = write_config(&VALID.replace(r#"environment = "testnet""#, ""));
    let err = settings::load(&path).expect_err("environment is absent");
    assert!(matches!(err, Error::InvalidConfig { .. }), "got {err:?}");
    assert!(
        source_chain(&err).contains("environment"),
        "must name the field: {}",
        source_chain(&err)
    );
}

#[test]
fn wrong_type_for_a_field_is_a_typed_error_not_a_panic() {
    let (_dir, path) =
        write_config(&VALID.replace("recv_window_ms = 5000", r#"recv_window_ms = "soon""#));
    let err = settings::load(&path).expect_err("recv_window_ms is not a number");
    assert!(matches!(err, Error::InvalidConfig { .. }), "got {err:?}");
    assert!(source_chain(&err).contains("recv_window_ms"));
}

#[test]
fn unknown_key_is_rejected_rather_than_silently_ignored() {
    // A typo in a config key must be loud. Silently ignoring `recv_window`
    // would leave the operator believing they had set something they had not.
    let (_dir, path) = write_config(&format!("{VALID}\n[extra]\nnope = 1\n"));
    let err = settings::load(&path).expect_err("unknown table must be rejected");
    assert!(matches!(err, Error::InvalidConfig { .. }), "got {err:?}");
    assert!(
        source_chain(&err).contains("extra"),
        "{}",
        source_chain(&err)
    );
}

#[test]
fn unknown_environment_value_is_rejected() {
    let (_dir, path) = write_config(&VALID.replace("testnet\"", "mainnet\""));
    let err = settings::load(&path).expect_err("`mainnet` is not an environment we know");
    assert!(matches!(err, Error::InvalidConfig { .. }), "got {err:?}");
}

#[test]
fn malformed_toml_is_a_typed_error_not_a_panic() {
    let (_dir, path) = write_config("environment = \"testnet\"\n[binance\n");
    let err = settings::load(&path).expect_err("this is not valid TOML");
    assert!(matches!(err, Error::InvalidConfig { .. }), "got {err:?}");
}

#[test]
fn out_of_range_recv_window_is_rejected() {
    for bad in ["0", "60001"] {
        let (_dir, path) = write_config(&VALID.replace("5000", bad));
        let err = settings::load(&path).expect_err("recv window out of range");
        assert!(
            matches!(&err, Error::InvalidValue { field, .. } if *field == "binance.recv_window_ms"),
            "got {err:?}"
        );
        assert!(err.to_string().contains("60000"), "{err}");
    }
}

#[test]
fn urls_must_carry_the_scheme_their_transport_requires() {
    let (_dir, path) = write_config(&VALID.replace("https://testnet", "http://testnet"));
    let err = settings::load(&path).expect_err("plaintext REST must be rejected");
    assert!(
        matches!(&err, Error::InvalidValue { field, .. } if *field == "binance.spot_rest_url"),
        "got {err:?}"
    );

    let (_dir, path) = write_config(&VALID.replace("wss://stream", "ws://stream"));
    let err = settings::load(&path).expect_err("plaintext websocket must be rejected");
    assert!(
        matches!(&err, Error::InvalidValue { field, .. } if *field == "binance.spot_ws_url"),
        "got {err:?}"
    );
}

#[test]
fn empty_logging_level_is_rejected() {
    let (_dir, path) = write_config(&VALID.replace(r#"level = "info""#, r#"level = "  ""#));
    let err = settings::load(&path).expect_err("blank level");
    assert!(
        matches!(&err, Error::InvalidValue { field, .. } if *field == "logging.level"),
        "got {err:?}"
    );
}

#[test]
fn config_debug_is_safe_to_log_in_full() {
    let config: Config = settings::load(repo_config()).expect("valid");
    let rendered = format!("{config:?}");
    // Everything in Config is non-secret by construction; assert the summary we
    // log actually shows the fields an operator needs to sanity-check a start-up.
    for expected in ["Testnet", "testnet.binance.vision", "5000", "info"] {
        assert!(
            rendered.contains(expected),
            "missing {expected} in {rendered}"
        );
    }
}

/// Flatten an error and its `source` chain into one string, since the useful
/// detail from figment lives on the source rather than the top-level message.
fn source_chain(err: &Error) -> String {
    let mut out = err.to_string();
    let mut cur: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(e) = cur {
        out.push_str(" | ");
        out.push_str(&e.to_string());
        cur = e.source();
    }
    out
}
