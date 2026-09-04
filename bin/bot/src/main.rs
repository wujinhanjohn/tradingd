//! The trading bot binary.
//!
//! Deliberately thin: parse args, load config and credentials, set up logging,
//! run the environment guard, build the market source and the engine, hand over.
//! All the behaviour lives in the library crates.
//!
//! This is the only crate in the workspace that names concrete implementations.
//! `engine` sees a `Box<dyn domain::Strategy>` and a channel of
//! `domain::IngestMsg` and nothing more - it depends on neither `strategy` nor
//! `exchange` - so swapping `NoopStrategy` for a real one, or the live feed for
//! a replay source, is a change to this file alone.

mod quantize_demo;

use std::sync::Arc;
use std::time::Duration;

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

    /// After fetching symbol filters, quantize a few example orders and log
    /// what the quantizer decided.
    ///
    /// Nothing is sent: `domain::quantize` is pure, and there is no order path
    /// until milestone 6. This exists so a real run can show the quantizer
    /// working against the filters the live exchange just returned.
    #[arg(long)]
    quantize_demo: bool,
}

/// The environment the config declares, as the exchange adapter's own type.
///
/// One `match`, in the one crate that is allowed to know both sides. This is
/// what keeps `exchange` free of a dependency on `settings` while still making
/// the endpoint cross-check structural: `BinanceMarketSource::connect` refuses
/// to build a source whose URL does not belong to the class passed here.
fn endpoint_class(environment: settings::Env) -> exchange::EndpointClass {
    match environment {
        settings::Env::Testnet => exchange::EndpointClass::Testnet,
        settings::Env::Production => exchange::EndpointClass::Production,
    }
}

/// A configured stream kind, as the exchange adapter's own type.
///
/// The same shape of mapping, for the same reason. `settings` spells these in
/// its own snake_case; Binance's wire spelling is the adapter's business.
fn stream_kind(kind: settings::StreamKind) -> exchange::StreamKind {
    match kind {
        settings::StreamKind::BookTicker => exchange::StreamKind::BookTicker,
        settings::StreamKind::Trade => exchange::StreamKind::Trade,
    }
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
        symbols = ?config.market.symbols,
        streams = ?config.market.streams,
        staleness_ms = config.market.staleness_ms,
        recording_enabled = config.recording.enabled,
        recording_dir = %config.recording.dir.display(),
        log_level = config.logging.level,
        log_json = config.logging.json,
        credentials = "loaded from environment (redacted)",
        "configuration"
    );

    // --- the market source: the second injection seam ---
    //
    // `connect` opens no socket. It validates that the configured URL actually
    // belongs to the declared environment, and that an enabled recording target
    // is writable, and only then hands back something that can be run. A
    // testnet-labelled config aimed at a production host fails here, before any
    // I/O, rather than connecting and warning.
    let symbols = config
        .market
        .symbols()
        .context("validating the configured market symbols")?;
    let kinds: Vec<exchange::StreamKind> = config
        .market
        .streams
        .iter()
        .copied()
        .map(stream_kind)
        .collect();
    let streams = exchange::StreamSet::new(&symbols, &kinds)
        .context("building the market subscription set")?;

    // The REST client: validated the same way, through the same gate, before it
    // can issue a single request. Built here, next to the source, because
    // everything that can be checked without I/O is checked before any I/O
    // happens - which is what keeps a mismatched endpoint from reaching the
    // network even by accident.
    let rest = Arc::new(
        exchange::RestClient::new(
            endpoint_class(config.environment),
            &config.binance.spot_rest_url,
        )
        .context("preparing the Binance REST client")?,
    );

    let (source, market_rx) = exchange::BinanceMarketSource::connect(
        endpoint_class(config.environment),
        &config.binance.spot_ws_url,
        exchange::SourceConfig {
            staleness: Duration::from_millis(config.market.staleness_ms),
            recording_dir: config
                .recording
                .enabled
                .then(|| config.recording.dir.clone()),
            ..exchange::SourceConfig::new(streams)
        },
    )
    .context("preparing the Binance market source")?;

    if let Some(path) = source.recording_path() {
        tracing::info!(recording = %path.display(), "recording this session");
    } else {
        tracing::warn!("recording is disabled; this session will leave nothing behind");
    }

    // --- symbol filters: the first I/O of the run, and it is fail-closed ---
    //
    // A symbol whose trading rules we cannot fetch is one we cannot safely place
    // an order on, so a failed fetch or a missing symbol refuses to start rather
    // than starting without rules and discovering it at the first order.
    let fetched = rest.fetch_exchange_info(&symbols).await.with_context(|| {
        format!(
            "fetching symbol filters from `{}`. Refusing to start without them",
            rest.base_url()
        )
    })?;

    for info in &fetched.symbols {
        let filters = &info.filters;
        tracing::info!(
            symbol = %filters.symbol(),
            status = info.status,
            tick_size = %filters.tick_size(),
            min_price = %filters.min_price(),
            max_price = %filters.max_price(),
            step_size = %filters.step_size(),
            min_qty = %filters.min_qty(),
            max_qty = %filters.max_qty(),
            min_notional = %filters.min_notional(),
            "symbol filters"
        );
        if info.status != exchange::STATUS_TRADING {
            tracing::warn!(
                symbol = %filters.symbol(),
                status = info.status,
                "the exchange is not currently trading this symbol"
            );
        }
        if !info.unmodeled.is_empty() {
            // Rules the exchange enforces and this build does not. They mean an
            // order the quantizer thinks is fine can still be rejected, and that
            // is much better said here than discovered at milestone 6.
            tracing::warn!(
                symbol = %filters.symbol(),
                unmodeled = ?info.unmodeled,
                "the exchange enforces filters this build does not model"
            );
        }
        if args.quantize_demo {
            quantize_demo::run(filters);
        }
    }

    // The refresh loop, and the handle milestone 6's order path will read. It
    // has no consumer yet beyond keeping the book current and logging changes -
    // the mechanism is what this milestone builds.
    let (refresher, filter_book_rx) = exchange::FilterRefresher::new(
        Arc::clone(&rest),
        symbols.clone(),
        Duration::from_millis(config.filters.refresh_interval_ms),
        Arc::clone(&fetched.book),
    );
    tracing::info!(
        refresh_interval_ms = config.filters.refresh_interval_ms,
        max_age_ms = config.filters.max_age_ms,
        symbols = fetched.book.len(),
        "symbol filters loaded; refreshing on an interval"
    );
    let refresh_task = tokio::spawn(refresher.run());

    let source_task = tokio::spawn(source.run());

    // The strategy seam. `engine` cannot name `NoopStrategy` - it does not depend
    // on the `strategy` crate at all - so a real strategy drops in right here with
    // no change anywhere else.
    let strategy: Box<dyn Strategy> = Box::new(strategy::NoopStrategy);
    tracing::info!(strategy = strategy.name(), "strategy loaded");

    let engine = engine::Engine::new(config, credentials, strategy, market_rx);

    // Order matters. `run` consumes the engine, so the receiver is dropped the
    // moment it returns - which is how the source learns to stop, flush its
    // recording, and finish. Awaiting the task afterwards is what makes the
    // recording durable before the process exits, and it is also how a source
    // that died on its own reports *why*: it closes the channel, the engine
    // stops fail-closed, and the real reason surfaces here.
    let engine_outcome = engine.run().await;
    let source_outcome = source_task
        .await
        .context("the market source task panicked")?;

    // The refresher has nothing to flush - it holds no file and no socket - so
    // it is aborted rather than waited for. Waiting would mean sitting through
    // the rest of a refresh interval on the way out.
    drop(filter_book_rx);
    refresh_task.abort();

    engine_outcome.context("engine stopped with an error")?;
    source_outcome.context("market source stopped with an error")?;

    Ok(())
}
