//! The REST layer, end to end against a **local fake HTTP server**.
//!
//! Offline and deterministic: a real TCP listener on `127.0.0.1:0`, real
//! HTTP/1.1, the real `RestClient`, the real parse - and never the live
//! exchange. Time enters through an injected clock, so freshness is asserted on
//! exact nanoseconds rather than on a sleep.
//!
//! What this cannot cover, and the milestone-2 lesson says to state plainly:
//! everything past the TCP connection on a `https://` URL. The fake server is
//! plaintext loopback, so certificate verification, the webpki root store and
//! the rustls handshake itself are exercised only by the manual testnet run.

mod fake_http;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use domain::{Decimal, Symbol};
use exchange::{Clock, EndpointClass, FilterRefresher, RestClient, RestError, STATUS_TRADING};
use fake_http::{FakeRest, Reply};

/// A clock that can be moved by hand. Freshness must be asserted, not slept for.
#[derive(Debug)]
struct StepClock(std::sync::atomic::AtomicI64);

impl StepClock {
    fn new(now_ns: i64) -> Arc<Self> {
        Arc::new(Self(std::sync::atomic::AtomicI64::new(now_ns)))
    }

    fn advance(&self, by_ns: i64) {
        self.0.fetch_add(by_ns, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for StepClock {
    fn now_ns(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// `Box<dyn Clock>` wrapper so one clock can be shared with the test.
#[derive(Debug)]
struct SharedClock(Arc<StepClock>);

impl Clock for SharedClock {
    fn now_ns(&self) -> i64 {
        self.0.now_ns()
    }
}

const SECOND_NS: i64 = 1_000_000_000;
const START_NS: i64 = 1_788_520_355_163_000_000;

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/exchange_info")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

fn sym(name: &str) -> Symbol {
    Symbol::new(name).expect("valid symbol")
}

fn client(base_url: &str, clock: &Arc<StepClock>) -> RestClient {
    RestClient::with_clock(
        EndpointClass::Testnet,
        base_url,
        Duration::from_secs(5),
        Box::new(SharedClock(Arc::clone(clock))),
    )
    .expect("a loopback base URL is accepted in the testnet direction")
}

#[tokio::test]
async fn a_successful_fetch_becomes_a_filter_book_stamped_with_the_fetch_time() {
    let server = FakeRest::start(vec![Reply::ok(fixture("btcusdt.json"))]).await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let fetched = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect("the fixture response must parse");

    assert_eq!(server.request_count(), 1);
    assert_eq!(
        server.targets(),
        vec!["/api/v3/exchangeInfo?symbol=BTCUSDT".to_owned()],
        "one symbol asks for one symbol, not for the whole market"
    );

    assert_eq!(fetched.book.fetched_at_ns(), START_NS);
    assert_eq!(fetched.book.symbols(), vec![sym("BTCUSDT")]);
    let filters = fetched.book.get(&sym("BTCUSDT")).expect("BTCUSDT filters");
    assert_eq!(filters.tick_size().0.to_string(), "0.01000000");
    assert_eq!(filters.min_notional().to_string(), "5.00000000");

    // The detail the book does not carry comes back alongside it, so the caller
    // can log what the exchange enforces and we do not.
    assert_eq!(fetched.symbols[0].status, STATUS_TRADING);
    assert!(fetched.symbols[0]
        .unmodeled
        .contains(&"PERCENT_PRICE_BY_SIDE".to_owned()));
}

#[tokio::test]
async fn several_symbols_are_asked_for_in_one_request() {
    let server = FakeRest::start(vec![Reply::ok(fixture("two_symbols.json"))]).await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let fetched = client
        .fetch_exchange_info(&[sym("BTCUSDT"), sym("ETHUSDT")])
        .await
        .expect("must parse");

    assert_eq!(server.request_count(), 1, "one request, not one per symbol");
    assert_eq!(
        server.targets(),
        vec!["/api/v3/exchangeInfo?symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D".to_owned()]
    );
    assert_eq!(fetched.book.len(), 2);
}

#[tokio::test]
async fn a_configured_symbol_missing_from_the_response_refuses() {
    // Fail-closed, and the reason the startup fetch is mandatory: a symbol whose
    // rules we cannot read is one we cannot safely place an order on.
    let server = FakeRest::start(vec![Reply::ok(fixture("btcusdt.json"))]).await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let err = client
        .fetch_exchange_info(&[sym("BTCUSDT"), sym("ETHUSDT")])
        .await
        .expect_err("must refuse");

    assert!(matches!(err, RestError::Filters(_)), "{err:?}");
    assert!(err.to_string().contains("ETHUSDT"), "{err}");
}

#[tokio::test]
async fn a_malformed_body_is_a_typed_decode_error() {
    let server = FakeRest::start(vec![Reply::ok("{not json at all")]).await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let err = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect_err("must refuse");
    assert!(matches!(err, RestError::Decode { .. }), "{err:?}");
}

#[tokio::test]
async fn a_binance_error_body_is_surfaced_with_its_code_and_message() {
    // What the live testnet really answers for an unknown symbol, verified
    // against it on 2026-09-04: HTTP 400 with `{"code":-1121,...}`.
    let server = FakeRest::start(vec![Reply::status(
        400,
        "Bad Request",
        r#"{"code":-1121,"msg":"Invalid symbol."}"#,
    )])
    .await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let err = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect_err("must refuse");

    let RestError::Status {
        status, code, msg, ..
    } = &err
    else {
        panic!("expected a Status error, got {err:?}");
    };
    assert_eq!(*status, 400);
    assert_eq!(*code, Some(-1121));
    assert_eq!(msg.as_deref(), Some("Invalid symbol."));
    assert!(err.to_string().contains("Invalid symbol."), "{err}");
}

#[tokio::test]
async fn a_redirect_is_refused_rather_than_followed_off_the_validated_host() {
    // A 3xx is a request to go somewhere the endpoint gate never saw. Following
    // one would take the validated host out from under us.
    let server = FakeRest::start(vec![Reply::status(302, "Found", "")]).await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let err = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect_err("must refuse");
    assert!(
        matches!(
            err,
            RestError::Status { status: 302, .. } | RestError::Http { .. }
        ),
        "{err:?}"
    );
    assert_eq!(server.request_count(), 1, "and it followed nothing");
}

#[tokio::test]
async fn an_endpoint_mismatch_refuses_with_zero_requests_reaching_the_server() {
    // The structural guarantee, proved by counting: the client cannot be
    // constructed, so there is nothing to make a request with. Both directions.
    let server = FakeRest::start(vec![Reply::ok(fixture("btcusdt.json"))]).await;

    for (expected, url) in [
        (EndpointClass::Testnet, "https://api.binance.com"),
        (EndpointClass::Production, "https://testnet.binance.vision"),
        // A loopback URL is the testnet carve-out only; production refuses it,
        // which is what stops a production run reading a fake exchange.
        (EndpointClass::Production, server.url()),
        // Plaintext against a real host is not negotiable either.
        (EndpointClass::Testnet, "http://testnet.binance.vision"),
    ] {
        let err = RestClient::new(expected, url).expect_err("must refuse");
        assert!(
            matches!(err, RestError::EndpointMismatch(_)),
            "{url}: {err:?}"
        );
    }

    assert_eq!(
        server.request_count(),
        0,
        "a refused endpoint must send nothing at all"
    );
}

#[tokio::test]
async fn freshness_is_judged_against_the_fetch_time_the_book_carries() {
    let server = FakeRest::start(vec![Reply::ok(fixture("btcusdt.json"))]).await;
    let clock = StepClock::new(START_NS);
    let client = client(server.url(), &clock);

    let fetched = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect("must parse");
    let max_age = 900 * SECOND_NS;

    assert!(fetched.book.ensure_fresh(clock.now_ns(), max_age).is_ok());
    clock.advance(899 * SECOND_NS);
    assert!(
        fetched.book.ensure_fresh(clock.now_ns(), max_age).is_ok(),
        "still inside the bound"
    );
    clock.advance(2 * SECOND_NS);
    assert!(
        fetched.book.ensure_fresh(clock.now_ns(), max_age).is_err(),
        "past the bound, and nothing refetched it"
    );
}

#[tokio::test]
async fn a_refresh_publishes_a_new_book_when_the_filters_change() {
    // The mechanism milestone 6 will read: the refresher owns the only writer,
    // and a swap is atomic, so a reader never sees half an update.
    let changed = fixture("btcusdt.json").replace("0.01000000", "0.05000000");
    let server =
        FakeRest::start(vec![Reply::ok(fixture("btcusdt.json")), Reply::ok(changed)]).await;
    let clock = StepClock::new(START_NS);
    let client = Arc::new(client(server.url(), &clock));

    let first = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect("must parse");
    assert_eq!(
        first
            .book
            .get(&sym("BTCUSDT"))
            .expect("filters")
            .tick_size()
            .0,
        Decimal::from_str_exact("0.01").expect("decimal")
    );

    let (refresher, mut rx) = FilterRefresher::new(
        Arc::clone(&client),
        vec![sym("BTCUSDT")],
        Duration::from_millis(20),
        Arc::clone(&first.book),
    );
    let task = tokio::spawn(refresher.run());

    clock.advance(60 * SECOND_NS);
    rx.changed().await.expect("the refresher must publish");

    let latest = rx.borrow_and_update().clone();
    assert_eq!(
        latest.get(&sym("BTCUSDT")).expect("filters").tick_size().0,
        Decimal::from_str_exact("0.05").expect("decimal"),
        "the new book carries the exchange's new tick size"
    );
    assert_eq!(
        latest.fetched_at_ns(),
        START_NS + 60 * SECOND_NS,
        "and is stamped with when it was fetched, not when it was published"
    );

    // Dropping every receiver is how the refresher learns to stop.
    drop(rx);
    task.await.expect("the refresh task must end cleanly");
}

#[tokio::test]
async fn a_failed_refresh_keeps_the_last_good_book_and_lets_it_age() {
    // The fail-closed shape: we keep serving the rules we have, but the clock
    // keeps running on them, so `ensure_fresh` eventually refuses. Serving stale
    // rules forever because the exchange was briefly unreachable is exactly the
    // fail-open behaviour this design refuses.
    let server = FakeRest::start(vec![
        Reply::ok(fixture("btcusdt.json")),
        Reply::status(503, "Service Unavailable", "{}"),
    ])
    .await;
    let clock = StepClock::new(START_NS);
    let client = Arc::new(client(server.url(), &clock));

    let first = client
        .fetch_exchange_info(&[sym("BTCUSDT")])
        .await
        .expect("must parse");

    let (refresher, rx) = FilterRefresher::new(
        Arc::clone(&client),
        vec![sym("BTCUSDT")],
        Duration::from_millis(20),
        Arc::clone(&first.book),
    );
    let task = tokio::spawn(refresher.run());

    // Wait for the failing refresh to have been attempted.
    while server.request_count() < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let held = rx.borrow().clone();
    assert_eq!(
        held.fetched_at_ns(),
        first.book.fetched_at_ns(),
        "the last good book is still being served"
    );
    assert!(
        held.get(&sym("BTCUSDT")).is_some(),
        "with its filters intact"
    );

    let max_age = 60 * SECOND_NS;
    assert!(held.ensure_fresh(START_NS, max_age).is_ok());
    assert!(
        held.ensure_fresh(START_NS + max_age, max_age).is_err(),
        "but it ages, and then it is refused"
    );

    drop(rx);
    task.await.expect("the refresh task must end cleanly");
}
