//! The workspace's single HTTP client, and the `exchangeInfo` fetch.
//!
//! Everything in this module is I/O; everything worth testing without one lives
//! in [`crate::filters`] (the parse) and [`crate::book`] (freshness). That split
//! is deliberate and is the same one [`crate::binance`] makes for the socket.
//!
//! # One client, one chokepoint
//!
//! Every request this bot makes goes through [`RestClient`]. Milestone 4's rate
//! limiter and request-weight accounting slot in *here*, in front of
//! [`RestClient::get_json`], rather than being sprinkled over call sites - which
//! is why the request path is a single private method even though only one
//! endpoint uses it today.
//!
//! # The endpoint gate, structurally
//!
//! [`RestClient::new`] validates the base URL through [`crate::endpoint`] - the
//! same parser and the same host lists the WebSocket URL goes through - and it
//! does so *before* an `Agent` exists. A client aimed at the wrong environment
//! cannot be constructed, so there is no object to make a request on and no
//! request to count. Redirects are disabled for the same reason: a 3xx is a
//! request to go somewhere the gate never saw, and following one would take the
//! validated host out from under us.
//!
//! # TLS
//!
//! `https://` needs a rustls crypto provider exactly as `wss://` does, and
//! rustls 0.23 panics inside the handshake rather than guessing one. The `ring`
//! provider is selected by feature and installed explicitly by
//! [`crate::binance::install_crypto_provider`], which this module calls before
//! its first request for the same belt-and-braces reason.
//!
//! # Public data only
//!
//! `exchangeInfo` is unauthenticated - confirmed against the current spot REST
//! documentation, and against the live testnet, which answered a bare GET with
//! no API key. No credential reaches this module, so there is nothing here for a
//! secret to leak from.

use std::sync::Arc;
use std::time::Duration;

use domain::{Symbol, SymbolFilters};
use serde_json::Value;
use tokio::sync::watch;

use crate::binance::install_crypto_provider;
use crate::book::FilterBook;
use crate::endpoint::{require_class_for, EndpointClass, EndpointError, Protocol};
use crate::filters::{parse_exchange_info, FilterParseError, SymbolInfo};
use crate::source::{Clock, SystemClock};

/// The documented spot path. Confirmed 2026-09-04.
const EXCHANGE_INFO_PATH: &str = "/api/v3/exchangeInfo";

/// How long a single request may take, end to end.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// A hard ceiling on a response body.
///
/// A per-symbol `exchangeInfo` answer is a couple of kilobytes; the whole market
/// is a few megabytes. Eight is far above anything we ask for and far below
/// anything that could exhaust a container.
const MAX_BODY_BYTES: u64 = 8 * 1024 * 1024;

/// What one `exchangeInfo` fetch produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeInfo {
    /// The rules, with the time they were fetched.
    pub book: Arc<FilterBook>,
    /// Per-symbol detail the book does not carry: trading status, the unmodelled
    /// filter types, `MARKET_LOT_SIZE`. In the order asked for.
    pub symbols: Vec<SymbolInfo>,
}

/// Every way a REST call can fail. All of them mean "no filters".
#[derive(thiserror::Error, Debug)]
pub enum RestError {
    #[error(transparent)]
    EndpointMismatch(#[from] EndpointError),

    #[error("GET {url} failed")]
    Http {
        url: String,
        #[source]
        source: Box<ureq::Error>,
    },

    #[error("GET {url} answered HTTP {status}{}", binance_detail(.code, .msg.as_deref()))]
    Status {
        url: String,
        status: u16,
        /// Binance's own error code, when the body carried one.
        code: Option<i64>,
        msg: Option<String>,
    },

    #[error("GET {url} returned a body that is not JSON")]
    Decode {
        url: String,
        #[source]
        source: serde_json::Error,
    },

    #[error(transparent)]
    Filters(#[from] FilterParseError),

    #[error("the blocking HTTP call did not finish")]
    Task(#[source] tokio::task::JoinError),
}

/// The validated HTTP client.
///
/// Constructing one proves the base URL belongs to the declared environment.
/// There is no way to build one that skips that check.
#[derive(Debug)]
pub struct RestClient {
    base_url: String,
    endpoint: EndpointClass,
    agent: ureq::Agent,
    clock: Box<dyn Clock>,
}

impl RestClient {
    /// Validate the base URL and build the client. Issues no request.
    ///
    /// # Errors
    ///
    /// [`RestError::EndpointMismatch`] when `base_url` does not belong to
    /// `expected`, in either direction, or is not a recognised endpoint at all.
    pub fn new(expected: EndpointClass, base_url: &str) -> Result<Self, RestError> {
        Self::with_clock(expected, base_url, DEFAULT_TIMEOUT, Box::new(SystemClock))
    }

    /// [`RestClient::new`] with the fetch clock and the timeout injected.
    ///
    /// Exists so tests can assert on exact `fetched_at_ns` values without a real
    /// clock. Production calls [`RestClient::new`].
    ///
    /// # Errors
    ///
    /// As [`RestClient::new`].
    pub fn with_clock(
        expected: EndpointClass,
        base_url: &str,
        timeout: Duration,
        clock: Box<dyn Clock>,
    ) -> Result<Self, RestError> {
        // First, before anything else, and before any I/O at all.
        require_class_for(Protocol::Rest, expected, base_url)?;

        // A `https://` handshake would otherwise panic mid-connect, exactly as
        // `wss://` did in milestone 2.
        install_crypto_provider();

        let config = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            // A redirect is a request to go somewhere `require_class_for` never
            // saw. Refuse to follow one rather than let a 3xx move us off the
            // validated host.
            .max_redirects(0)
            // Read the body ourselves on a non-2xx: Binance puts a typed
            // `{"code":..,"msg":..}` in it, and that is the most useful thing an
            // operator can be told.
            .http_status_as_error(false)
            .user_agent(concat!("trading-bot/", env!("CARGO_PKG_VERSION")))
            .build();

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            endpoint: expected,
            agent: config.into(),
            clock,
        })
    }

    /// The validated base URL, with any trailing slash trimmed.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The environment this client is pinned to.
    #[must_use]
    pub fn endpoint(&self) -> EndpointClass {
        self.endpoint
    }

    /// Fetch and parse the filters for exactly these symbols.
    ///
    /// A symbol missing from the response is [`FilterParseError::SymbolNotListed`]
    /// rather than a gap in the book: a symbol whose rules we cannot read is one
    /// we cannot safely place an order on.
    ///
    /// # Errors
    ///
    /// [`RestError`]. Every variant means the caller has no filters, which at
    /// startup means refusing to start.
    pub async fn fetch_exchange_info(&self, symbols: &[Symbol]) -> Result<ExchangeInfo, RestError> {
        let url = format!(
            "{}{EXCHANGE_INFO_PATH}{}",
            self.base_url,
            symbol_query(symbols)
        );
        let payload = self.get_json(&url).await?;
        let parsed = parse_exchange_info(&payload, symbols)?;

        let filters: std::collections::HashMap<Symbol, SymbolFilters> = parsed
            .iter()
            .map(|info| (info.filters.symbol().clone(), info.filters.clone()))
            .collect();

        Ok(ExchangeInfo {
            book: Arc::new(FilterBook::new(filters, self.clock.now_ns())),
            symbols: parsed,
        })
    }

    /// The one request path. Milestone 4's rate limiter goes in front of this.
    async fn get_json(&self, url: &str) -> Result<Value, RestError> {
        let agent = self.agent.clone();
        let target = url.to_owned();

        // `ureq` is blocking, which is the right shape for a startup fetch and a
        // periodic refresh - and keeps hyper's tree out of the workspace. It just
        // must not run on a runtime thread.
        let response = tokio::task::spawn_blocking(move || {
            let mut response = agent.get(&target).call()?;
            let status = response.status().as_u16();
            let body = response
                .body_mut()
                .with_config()
                .limit(MAX_BODY_BYTES)
                .read_to_string()?;
            Ok::<_, ureq::Error>((status, body))
        })
        .await
        .map_err(RestError::Task)?;

        let (status, body) = response.map_err(|source| RestError::Http {
            url: url.to_owned(),
            source: Box::new(source),
        })?;

        if status != 200 {
            // Binance answers a bad request with `{"code":-1121,"msg":"Invalid
            // symbol."}`. Surface that rather than a bare status number.
            let error: Option<Value> = serde_json::from_str(&body).ok();
            return Err(RestError::Status {
                url: url.to_owned(),
                status,
                code: error.as_ref().and_then(|v| v.get("code")?.as_i64()),
                msg: error
                    .as_ref()
                    .and_then(|v| Some(v.get("msg")?.as_str()?.to_owned())),
            });
        }

        serde_json::from_str(&body).map_err(|source| RestError::Decode {
            url: url.to_owned(),
            source,
        })
    }
}

/// Binance's own error code and message, rendered for a log line, or nothing at
/// all when the body carried neither.
fn binance_detail(code: &Option<i64>, msg: Option<&str>) -> String {
    match (code, msg) {
        (Some(code), Some(msg)) => format!(" (Binance code {code}: {msg})"),
        (Some(code), None) => format!(" (Binance code {code})"),
        (None, Some(msg)) => format!(" ({msg})"),
        (None, None) => String::new(),
    }
}

/// The query string for a symbol subset.
///
/// One symbol uses `?symbol=`, several use `?symbols=["A","B"]` - both
/// documented, and the second needs percent-encoding. Encoding is done by hand
/// for the four characters involved rather than by pulling in a URL crate:
/// [`domain::Symbol`] guarantees the symbols themselves are uppercase ASCII
/// alphanumeric, so nothing else in this string can need escaping.
///
/// An empty list would ask for the whole market, which is megabytes and 20
/// weight; the caller never has one (a config with no symbols does not load),
/// and if it somehow did, asking for everything is the wrong answer - so an
/// empty list yields a query for nothing, and the parse then refuses.
fn symbol_query(symbols: &[Symbol]) -> String {
    match symbols {
        [] => String::new(),
        [one] => format!("?symbol={one}"),
        many => {
            let names: Vec<String> = many.iter().map(|s| format!("%22{s}%22")).collect();
            format!("?symbols=%5B{}%5D", names.join("%2C"))
        }
    }
}

/// Keeps the filter book fresh, and publishes each new one.
///
/// Owns the only writer to the [`watch`] channel. Readers - milestone 6's order
/// path - hold a receiver and always see the newest whole book; a swap is
/// atomic, so nobody can observe half an update.
///
/// On a failed refresh the last good book keeps being served and the failure is
/// logged, **but `fetched_at_ns` does not move**. That is the point: the book
/// ages, and [`FilterBook::ensure_fresh`] starts refusing once it ages past the
/// configured bound. Serving stale rules indefinitely because the exchange was
/// briefly unreachable is precisely the fail-open behaviour this design refuses.
pub struct FilterRefresher {
    client: Arc<RestClient>,
    symbols: Vec<Symbol>,
    interval: Duration,
    tx: watch::Sender<Arc<FilterBook>>,
}

impl FilterRefresher {
    /// Build a refresher around an already-fetched book, and hand back the
    /// receiver that sees every later one.
    #[must_use]
    pub fn new(
        client: Arc<RestClient>,
        symbols: Vec<Symbol>,
        interval: Duration,
        initial: Arc<FilterBook>,
    ) -> (Self, watch::Receiver<Arc<FilterBook>>) {
        let (tx, rx) = watch::channel(initial);
        (
            Self {
                client,
                symbols,
                interval,
                tx,
            },
            rx,
        )
    }

    /// Refetch on a fixed interval until every receiver has gone away.
    ///
    /// Never hot-loops: the sleep comes first and happens on every path,
    /// including a failed fetch.
    pub async fn run(self) {
        loop {
            tokio::time::sleep(self.interval).await;

            if self.tx.is_closed() {
                tracing::debug!("nothing is reading the filter book; stopping the refresh");
                return;
            }

            match self.client.fetch_exchange_info(&self.symbols).await {
                Ok(fetched) => {
                    log_changes(&self.tx.borrow().clone(), &fetched);
                    if self.tx.send(Arc::clone(&fetched.book)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    // The last good book stays in the channel and keeps ageing.
                    tracing::warn!(
                        error = %error,
                        cause = ?std::error::Error::source(&error),
                        "could not refresh symbol filters; serving the last good book, \
                         which is now ageing towards its freshness bound"
                    );
                }
            }
        }
    }
}

/// Log what a refresh changed.
///
/// A tick, step or notional moving under a running bot is exactly the event
/// freshness exists to catch, so it is a `warn`, not a `debug`: every order
/// quantized before this moment used different rules.
fn log_changes(previous: &FilterBook, fetched: &ExchangeInfo) {
    for info in &fetched.symbols {
        let symbol = info.filters.symbol();
        match previous.get(symbol) {
            Some(before) if *before == info.filters => {}
            Some(before) => tracing::warn!(
                symbol = %symbol,
                tick_size = %before.tick_size(), new_tick_size = %info.filters.tick_size(),
                step_size = %before.step_size(), new_step_size = %info.filters.step_size(),
                min_qty = %before.min_qty(), new_min_qty = %info.filters.min_qty(),
                min_notional = %before.min_notional(),
                new_min_notional = %info.filters.min_notional(),
                min_price = %before.min_price(), new_min_price = %info.filters.min_price(),
                max_price = %before.max_price(), new_max_price = %info.filters.max_price(),
                max_qty = %before.max_qty(), new_max_qty = %info.filters.max_qty(),
                "symbol filters changed at the exchange"
            ),
            None => tracing::warn!(
                symbol = %symbol,
                "symbol filters arrived for a symbol the previous book did not carry"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym(name: &str) -> Symbol {
        Symbol::new(name).expect("valid symbol")
    }

    #[test]
    fn one_symbol_uses_the_simple_query_and_several_use_the_encoded_list() {
        assert_eq!(symbol_query(&[]), "");
        assert_eq!(symbol_query(&[sym("BTCUSDT")]), "?symbol=BTCUSDT");
        assert_eq!(
            symbol_query(&[sym("BTCUSDT"), sym("ETHUSDT")]),
            "?symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D"
        );
        // Which is `?symbols=["BTCUSDT","ETHUSDT"]`, the documented spelling.
    }

    #[test]
    fn a_base_url_keeps_its_path_but_loses_a_trailing_slash() {
        // Doubling the slash would give `//api/v3/exchangeInfo`, which some
        // proxies answer and some do not. Normalise once, at construction.
        let client = RestClient::new(EndpointClass::Testnet, "https://testnet.binance.vision/")
            .expect("a valid testnet base URL");
        assert_eq!(client.base_url(), "https://testnet.binance.vision");
        assert_eq!(client.endpoint(), EndpointClass::Testnet);
    }

    #[test]
    fn a_client_aimed_at_the_wrong_environment_cannot_be_constructed() {
        // Both directions, and the dangerous one first: no object means no
        // request, which is what makes this structural rather than a check
        // someone can forget to call.
        let err = RestClient::new(EndpointClass::Testnet, "https://api.binance.com")
            .expect_err("a testnet config must never reach production");
        assert!(matches!(err, RestError::EndpointMismatch(_)), "{err:?}");
        assert!(err.to_string().contains("refusing to connect"), "{err}");

        assert!(
            RestClient::new(EndpointClass::Production, "https://testnet.binance.vision").is_err()
        );

        // And the scheme is not negotiable against a real host.
        assert!(RestClient::new(EndpointClass::Testnet, "http://testnet.binance.vision").is_err());
        // Nor is a WebSocket URL a REST base URL.
        assert!(RestClient::new(
            EndpointClass::Testnet,
            "wss://stream.testnet.binance.vision/stream"
        )
        .is_err());
    }
}
