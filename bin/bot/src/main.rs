//! The trading bot binary.
//!
//! Deliberately thin: parse args, load config and credentials, set up logging,
//! run the environment guard, build the engine, hand over. All the behaviour
//! lives in the library crates.
//!
//! This is the only crate in the workspace that names a concrete strategy. The
//! engine sees a `Box<dyn domain::Strategy>` and nothing more, so swapping
//! `NoopStrategy` for a real one is a change to this file alone.

use anyhow::Context;
use clap::Parser;
use domain::Strategy;

#[derive(Parser, Debug)]
#[command(
    name = "bot",
    version,
    about = "Binance spot trading bot",
    long_about = "Runs against Binance spot testnet by default. \
                  Credentials come from BINANCE_API_KEY and BINANCE_API_SECRET; \
                  see .env.example."
)]
struct Cli {
    /// Path to the TOML config file.
    #[arg(long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    // Config first: it carries the logging settings, so nothing before this point
    // can be logged. A failure here goes to stderr through anyhow and exits non-zero.
    let config = settings::load(&args.config)
        .with_context(|| format!("loading configuration from `{}`", args.config))?;

    engine::logging::init(&config.logging).context("initialising logging")?;

    let credentials = settings::load_credentials()
        .context("loading exchange credentials from the environment")?;

    // Loud guard: production must be explicit and confirmed. Runs after logging is
    // up so the refusal - or the production banner - is actually visible.
    engine::guard_environment(&config)?;

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        environment = %config.environment,
        config_path = args.config,
        "starting"
    );
    // Redacted by construction, not by filtering: `Config` has no field that can
    // hold a secret, and credentials are a separate type that never lands here.
    tracing::info!(
        environment = %config.environment,
        spot_rest_url = config.binance.spot_rest_url,
        spot_ws_url = config.binance.spot_ws_url,
        recv_window_ms = config.binance.recv_window_ms,
        log_level = config.logging.level,
        log_json = config.logging.json,
        credentials = "loaded from environment (redacted)",
        "configuration"
    );

    // The injection seam. `engine` cannot name `NoopStrategy` - it does not depend
    // on the `strategy` crate at all - so a real strategy drops in right here with
    // no change anywhere else.
    let strategy: Box<dyn Strategy> = Box::new(strategy::NoopStrategy);
    tracing::info!(strategy = strategy.name(), "strategy loaded");

    let engine = engine::Engine::new(config, credentials, strategy);
    engine.run().await.context("engine stopped with an error")?;

    Ok(())
}
