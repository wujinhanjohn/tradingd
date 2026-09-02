//! The connection layer, driven against a local fake WebSocket server.
//!
//! Nothing here touches the live testnet. Every scenario a live feed would
//! produce only by luck - a missed trade id, a silent stream, a mid-session
//! disconnect, a server ping - is scripted here and therefore happens on every
//! run, in the same order, in milliseconds.
//!
//! Staleness is tested under `tokio`'s paused virtual time, so a ten-second
//! bound costs no wall-clock time and cannot flake on a loaded machine.

mod fake_ws;

use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use domain::{Decimal, IngestMsg, MarketEvent, Price, Qty, Symbol};
use exchange::{
    BinanceMarketSource, Clock, EndpointClass, EndpointError, Marker, SourceConfig, SourceError,
    StreamKind, StreamSet,
};
use fake_ws::{book_ticker, data, trade, Act, FakeServer};
use tokio::sync::mpsc::Receiver;

// --- fixtures ---

/// A stopped clock. Makes the ingest timestamp a known constant, so a book
/// ticker's `event_time` - which has no exchange-side time and can only come
/// from our own ingest clock - is exactly assertable.
#[derive(Debug)]
struct FixedClock(i64);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// A clock that advances one millisecond per read. Deterministic, monotonic, and
/// unrelated to the machine's actual clock.
#[derive(Debug)]
struct StepClock(AtomicI64);

impl StepClock {
    fn new() -> Self {
        Self(AtomicI64::new(1_700_000_000_000_000_000))
    }
}

impl Clock for StepClock {
    fn now_ns(&self) -> i64 {
        self.0.fetch_add(1_000_000, Ordering::SeqCst)
    }
}

/// Parse a decimal exactly, the way the normalizer does. `Decimal`'s `PartialEq`
/// ignores scale (`1.50 == 1.5`), so tests that care about precision compare the
/// rendered string as well.
fn decimal(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).expect("an exact decimal")
}

fn symbol(raw: &str) -> Symbol {
    Symbol::new(raw).expect("valid symbol")
}

fn streams(kinds: &[StreamKind]) -> StreamSet {
    StreamSet::new(&[symbol("BTCUSDT")], kinds).expect("a valid stream set")
}

/// Test defaults: everything short, so a scenario that would take a minute
/// against real timings takes milliseconds. Jitter stays on - a schedule tested
/// without it is not the schedule that runs.
fn config(kinds: &[StreamKind]) -> SourceConfig {
    SourceConfig {
        staleness: Duration::from_secs(10),
        backoff: exchange::Backoff::new(
            Duration::from_millis(10),
            Duration::from_millis(40),
            2,
            20,
        )
        .expect("a valid schedule"),
        connect_timeout: Duration::from_secs(5),
        ack_timeout: Duration::from_secs(5),
        ..SourceConfig::new(streams(kinds))
    }
}

/// Start a source against `server`, returning the receiver and the task handle.
fn spawn(
    server: &FakeServer,
    config: SourceConfig,
    clock: Box<dyn Clock>,
) -> (
    Receiver<IngestMsg>,
    tokio::task::JoinHandle<Result<(), SourceError>>,
) {
    let (source, rx) = BinanceMarketSource::connect_with_clock(
        EndpointClass::Testnet,
        server.url(),
        config,
        clock,
    )
    .expect("a loopback URL is accepted under testnet");
    let source = source.with_jitter_seed(0xC0FF_EE00);
    (rx, tokio::spawn(source.run()))
}

/// Receive one message, failing the test rather than hanging if none comes.
async fn next(rx: &mut Receiver<IngestMsg>) -> IngestMsg {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("a message within ten seconds")
        .expect("the channel is still open")
}

fn market(message: IngestMsg) -> MarketEvent {
    match message {
        IngestMsg::Market(event) => event,
        other => panic!("expected a market event, got {other:?}"),
    }
}

// --- normalization over the wire ---

#[tokio::test]
async fn known_frames_arrive_as_exactly_normalized_market_events() {
    const RECV_NS: i64 = 1_712_345_678_901_234_567;

    let server = FakeServer::scripted(vec![
        Act::Ack,
        data(
            "btcusdt@bookTicker",
            &book_ticker("BTCUSDT", 400_900_217, "25.35190000", "25.36520000"),
        ),
        data(
            "btcusdt@trade",
            &trade("BTCUSDT", 12_345, 1_672_515_782_136, "0.00100000", "100"),
        ),
        Act::Hold,
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        config(StreamKind::ALL),
        Box::new(FixedClock(RECV_NS)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);

    let MarketEvent::BookTicker(book) = market(next(&mut rx).await) else {
        panic!("expected a book ticker");
    };
    assert_eq!(book.symbol, symbol("BTCUSDT"));
    // Parsed straight from the JSON strings Binance sends. Exact, not near.
    assert_eq!(book.bid, Price(decimal("25.35190000")));
    assert_eq!(book.ask, Price(decimal("25.36520000")));
    assert_eq!(book.bid_qty, Qty(decimal("1.00000000")));
    assert_eq!(book.ask_qty, Qty(decimal("2.00000000")));
    // A book ticker payload carries no exchange time at all, so the only honest
    // timestamp is our ingest clock, truncated to the domain's milliseconds.
    assert_eq!(book.event_time, RECV_NS / 1_000_000);

    let MarketEvent::Trade(trade) = market(next(&mut rx).await) else {
        panic!("expected a trade");
    };
    assert_eq!(trade.price, Price(decimal("0.00100000")));
    assert_eq!(trade.qty, Qty(decimal("100")));
    // A trade does carry its own event time, so that is what is used.
    assert_eq!(trade.event_time, 1_672_515_782_136);

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

#[tokio::test]
async fn the_subscribe_request_names_every_configured_stream() {
    let server = FakeServer::scripted(vec![Act::Ack, Act::Hold]).await;
    let (mut rx, handle) = spawn(
        &server,
        config(StreamKind::ALL),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    server.await_connections(1, Duration::from_secs(5)).await;

    assert_eq!(
        server.await_requests(0, 1, Duration::from_secs(5)).await,
        vec![r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker","btcusdt@trade"],"id":1}"#]
    );

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

// --- gaps, under each stream's own policy ---

#[tokio::test]
async fn a_missing_run_of_trade_ids_is_reported_as_a_countable_gap() {
    let server = FakeServer::scripted(vec![
        Act::Ack,
        data(
            "btcusdt@trade",
            &trade("BTCUSDT", 100, 1_672_515_782_000, "10", "1"),
        ),
        data(
            "btcusdt@trade",
            &trade("BTCUSDT", 137, 1_672_515_782_100, "11", "1"),
        ),
        Act::Hold,
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        config(&[StreamKind::Trade]),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    market(next(&mut rx).await);

    // The gap is emitted *before* the message on its far side, matching the
    // order the recording stores them in, so a replay sees the same interleaving.
    assert_eq!(
        next(&mut rx).await,
        IngestMsg::Gap {
            stream: "btcusdt@trade".to_owned(),
            detail: "missed on a contiguous stream: 100 -> 137, 36 message(s) lost".to_owned(),
        }
    );
    let MarketEvent::Trade(trade) = market(next(&mut rx).await) else {
        panic!("expected a trade");
    };
    assert_eq!(trade.price, Price(decimal("11")));

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

#[tokio::test]
async fn a_leaping_book_ticker_update_id_is_not_reported_as_a_gap() {
    // The reason the policy split exists. `u` is the order book updateId: it
    // counts book updates, not pushed messages, so it leaps as a matter of
    // course. Alerting on that would fire on nearly every message.
    let server = FakeServer::scripted(vec![
        Act::Ack,
        data("btcusdt@bookTicker", &book_ticker("BTCUSDT", 1, "10", "11")),
        data(
            "btcusdt@bookTicker",
            &book_ticker("BTCUSDT", 999_999, "12", "13"),
        ),
        data(
            "btcusdt@bookTicker",
            &book_ticker("BTCUSDT", 1_000_000, "14", "15"),
        ),
        Act::Hold,
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        config(&[StreamKind::BookTicker]),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    for expected in [decimal("10"), decimal("12"), decimal("14")] {
        let MarketEvent::BookTicker(book) = market(next(&mut rx).await) else {
            panic!("expected a book ticker");
        };
        assert_eq!(
            book.bid,
            Price(expected),
            "three book tickers in a row, with no gap between them"
        );
    }

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

// --- staleness, under paused virtual time ---

#[tokio::test]
async fn a_stream_that_goes_silent_is_reported_as_stale() {
    let server = FakeServer::scripted(vec![
        Act::Ack,
        data("btcusdt@bookTicker", &book_ticker("BTCUSDT", 1, "10", "11")),
        Act::Hold,
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        SourceConfig {
            // Comfortably shorter than `next`'s receive timeout: tokio's timer
            // has millisecond granularity, so two deadlines set from the same
            // instant would land in the same slot and race.
            staleness: Duration::from_secs(3),
            ..config(StreamKind::ALL)
        },
        Box::new(FixedClock(1_712_000_000_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    market(next(&mut rx).await);

    // Only now: real time until the connection is up and the first message has
    // landed, then virtual time so the staleness bound costs no wall clock.
    tokio::time::pause();

    // The trade stream never spoke at all, and the book ticker spoke once. Both
    // go stale; the order between them depends on sub-millisecond arrival times,
    // so assert the set rather than the sequence.
    let mut stale: Vec<String> = Vec::new();
    while stale.len() < 2 {
        match next(&mut rx).await {
            IngestMsg::Stale { stream, since_ns } => {
                assert!(since_ns > 0, "a stale report names when we last heard");
                stale.push(stream);
            }
            other => panic!("expected a stale report, got {other:?}"),
        }
    }
    stale.sort();
    assert_eq!(stale, vec!["btcusdt@bookTicker", "btcusdt@trade"]);

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

// --- reconnect ---

#[tokio::test]
async fn a_dropped_connection_reconnects_and_flags_the_gap_across_the_outage() {
    let server = FakeServer::start(vec![
        vec![
            Act::Ack,
            data(
                "btcusdt@trade",
                &trade("BTCUSDT", 100, 1_672_515_782_000, "10", "1"),
            ),
            Act::Drop,
        ],
        vec![
            Act::Ack,
            data(
                "btcusdt@trade",
                &trade("BTCUSDT", 105, 1_672_515_790_000, "11", "1"),
            ),
            Act::Hold,
        ],
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        config(&[StreamKind::Trade]),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    market(next(&mut rx).await);

    let IngestMsg::Disconnected { reason } = next(&mut rx).await else {
        panic!("expected a disconnect");
    };
    assert!(!reason.is_empty(), "a disconnect always says why");

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    // The outage is flagged against the last id seen before it, under the trade
    // stream's own contiguous policy - so it can say how much was lost.
    assert_eq!(
        next(&mut rx).await,
        IngestMsg::Gap {
            stream: "btcusdt@trade".to_owned(),
            detail: "outage on a contiguous stream: 100 -> 105, 4 message(s) lost".to_owned(),
        }
    );
    market(next(&mut rx).await);

    assert_eq!(server.connection_count(), 2);
    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

#[tokio::test]
async fn a_subscription_that_is_never_acknowledged_is_treated_as_a_bad_connection() {
    let server = FakeServer::start(vec![
        // Connects, accepts the SUBSCRIBE, and then never answers it.
        vec![Act::Hold],
        vec![
            Act::Ack,
            data("btcusdt@bookTicker", &book_ticker("BTCUSDT", 1, "10", "11")),
            Act::Hold,
        ],
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        SourceConfig {
            ack_timeout: Duration::from_millis(50),
            ..config(&[StreamKind::BookTicker])
        },
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    let IngestMsg::Disconnected { reason } = next(&mut rx).await else {
        panic!("expected a disconnect");
    };
    assert!(
        reason.contains("acknowledge"),
        "the reason should name the unacknowledged subscription: {reason}"
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    market(next(&mut rx).await);

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

// --- keepalive ---

#[tokio::test]
async fn a_server_ping_is_answered_with_a_pong_carrying_its_payload() {
    // The docs are explicit that only a pong echoing the ping's payload counts
    // as keepalive - an unsolicited empty pong is permitted and does nothing.
    // Tungstenite queues the right reply on read, but queued is not sent, so
    // this asserts the pong actually reached the wire rather than trusting the
    // library's word for it.
    const PAYLOAD: &[u8] = b"binance-keepalive";

    let server = FakeServer::scripted(vec![
        Act::Ack,
        Act::Ping(PAYLOAD),
        // Gated on the pong: the data frame only goes out once the client has
        // answered, so receiving the market event proves the pong arrived.
        Act::AwaitPongs(1),
        data("btcusdt@bookTicker", &book_ticker("BTCUSDT", 1, "10", "11")),
        Act::Hold,
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        config(&[StreamKind::BookTicker]),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    market(next(&mut rx).await);

    let pongs = server.await_pongs(0, 1, Duration::from_secs(5)).await;
    assert_eq!(
        pongs[0], PAYLOAD,
        "the pong must echo the ping's payload, or it does not count as keepalive"
    );

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");
}

// --- TLS ---

#[tokio::test]
async fn a_wss_url_attempts_a_real_tls_handshake_rather_than_panicking() {
    // The offline suite had a real blind spot: every other test here uses
    // plaintext `ws://` loopback, so the first `wss://` handshake in this
    // project's life happened against the live testnet - where rustls 0.23
    // panicked because no crypto provider had been chosen. A panic inside a
    // spawned source task is a far worse failure than a typed refusal.
    //
    // This closes the blind spot: a plain TCP server that accepts and hangs up
    // is enough to make the client walk the whole TLS setup path, after which it
    // fails as an ordinary connection error. What is asserted is the absence of
    // a panic, not the particular error.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback listener");
    let port = listener.local_addr().expect("a bound address").port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            drop(stream);
        }
    });

    let url = format!("wss://127.0.0.1:{port}/stream");
    let (source, mut rx) = BinanceMarketSource::connect_with_clock(
        EndpointClass::Testnet,
        &url,
        config(&[StreamKind::Trade]),
        Box::new(FixedClock(1_000_000_000)),
    )
    .expect("a loopback URL is accepted under testnet");
    let handle = tokio::spawn(source.with_jitter_seed(3).run());

    let IngestMsg::Disconnected { reason } = next(&mut rx).await else {
        panic!("expected a typed disconnect");
    };
    assert!(!reason.is_empty(), "a disconnect always says why");

    drop(rx);
    handle
        .await
        .expect("the source task must fail as an error, never as a panic")
        .expect("a clean stop");
}

// --- the endpoint gate, structurally ---

#[tokio::test]
async fn a_production_labelled_source_never_opens_a_loopback_socket() {
    let server = FakeServer::scripted(vec![Act::Ack, Act::Hold]).await;

    let error = BinanceMarketSource::connect(
        EndpointClass::Production,
        server.url(),
        config(StreamKind::ALL),
    )
    .expect_err("loopback is only ever allowed under testnet");

    assert!(matches!(
        error,
        SourceError::Endpoint(EndpointError::Unrecognised { .. })
    ));
    // The check runs before any I/O, so the listener never saw a handshake.
    assert_eq!(
        server.connection_count(),
        0,
        "a refused endpoint must not reach a socket at all"
    );
}

#[tokio::test]
async fn a_testnet_labelled_source_refuses_a_production_host() {
    // The direction that must never regress: a config saying "testnet" pointed
    // at the live exchange would pass the environment guard and trade real funds.
    let error = BinanceMarketSource::connect(
        EndpointClass::Testnet,
        "wss://stream.binance.com:9443/stream",
        config(StreamKind::ALL),
    )
    .expect_err("a production host under a testnet label must refuse");

    let SourceError::Endpoint(EndpointError::Mismatch {
        expected, found, ..
    }) = error
    else {
        panic!("expected a mismatch");
    };
    assert_eq!(expected, EndpointClass::Testnet);
    assert_eq!(found, EndpointClass::Production);
}

#[tokio::test]
async fn a_production_labelled_source_refuses_a_testnet_host() {
    let error = BinanceMarketSource::connect(
        EndpointClass::Production,
        exchange::TESTNET_SPOT_WS_URL,
        config(StreamKind::ALL),
    )
    .expect_err("a testnet host under a production label must refuse");

    assert!(matches!(
        error,
        SourceError::Endpoint(EndpointError::Mismatch {
            expected: EndpointClass::Production,
            found: EndpointClass::Testnet,
            ..
        })
    ));
}

// --- fail-closed ---

#[tokio::test]
async fn a_refused_subscription_stops_the_source_rather_than_retrying_forever() {
    // Retrying a refusal produces a process that is up, connected, and receiving
    // nothing - which looks healthier than being down and is far worse.
    let server = FakeServer::scripted(vec![
        Act::Text(r#"{"id":1,"error":{"code":2,"msg":"Invalid request: bad stream"}}"#.to_owned()),
        Act::Hold,
    ])
    .await;

    let (mut rx, handle) = spawn(
        &server,
        config(StreamKind::ALL),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);

    let error = handle
        .await
        .expect("joined")
        .expect_err("a refused subscription is fatal");
    let SourceError::SubscriptionRejected { code, msg, .. } = error else {
        panic!("expected a rejection");
    };
    assert_eq!(code, 2);
    assert_eq!(msg, "Invalid request: bad stream");

    // The channel closes, which is what the engine treats as a reason to stop.
    assert_eq!(rx.recv().await, None);
    assert_eq!(server.connection_count(), 1, "no retry after a refusal");
}

#[tokio::test]
async fn the_source_stops_when_the_engine_drops_the_receiver() {
    let server = FakeServer::scripted(vec![Act::Ack, Act::Hold]).await;
    let (mut rx, handle) = spawn(
        &server,
        config(StreamKind::ALL),
        Box::new(FixedClock(1_000_000_000)),
    );

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    drop(rx);

    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("the source must not hang when its consumer goes away")
        .expect("joined")
        .expect("a clean stop");
}

#[tokio::test]
async fn recording_into_an_unwritable_directory_refuses_to_start() {
    let dir = tempfile::tempdir().expect("a temp dir");
    // A file where the directory should be: unusable, and unusable at startup.
    let path = dir.path().join("not-a-directory");
    std::fs::write(&path, b"occupied").expect("write the blocking file");

    let server = FakeServer::scripted(vec![Act::Ack, Act::Hold]).await;
    let error = BinanceMarketSource::connect(
        EndpointClass::Testnet,
        server.url(),
        SourceConfig {
            recording_dir: Some(path),
            ..config(StreamKind::ALL)
        },
    )
    .expect_err("an unwritable recording target must refuse to start");

    assert!(matches!(error, SourceError::Recording(_)));
    assert_eq!(
        server.connection_count(),
        0,
        "refusing to record means refusing to run, not running unrecorded"
    );
}

// --- the recording the live path actually writes ---

#[tokio::test]
async fn a_live_session_records_a_file_that_replays_to_the_same_events_and_markers() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let server = FakeServer::start(vec![
        vec![
            Act::Ack,
            data(
                "btcusdt@trade",
                &trade("BTCUSDT", 100, 1_672_515_782_000, "10.5", "1"),
            ),
            Act::Drop,
        ],
        vec![
            Act::Ack,
            data(
                "btcusdt@trade",
                &trade("BTCUSDT", 105, 1_672_515_790_000, "11.5", "2"),
            ),
            Act::Hold,
        ],
    ])
    .await;

    let (source, mut rx) = BinanceMarketSource::connect_with_clock(
        EndpointClass::Testnet,
        server.url(),
        SourceConfig {
            recording_dir: Some(dir.path().to_path_buf()),
            ..config(&[StreamKind::Trade])
        },
        Box::new(StepClock::new()),
    )
    .expect("a loopback URL is accepted under testnet");
    let path: PathBuf = source
        .recording_path()
        .expect("recording is enabled")
        .to_path_buf();
    let handle = tokio::spawn(source.with_jitter_seed(7).run());

    // Drive the session through a drop and a reconnect.
    let mut live: Vec<IngestMsg> = Vec::new();
    while live.len() < 6 {
        live.push(next(&mut rx).await);
    }
    drop(rx);
    handle.await.expect("joined").expect("a clean stop");

    let (header, records) = exchange::read_session(&path).expect("a readable recording");
    assert_eq!(header.endpoint, EndpointClass::Testnet);
    assert_eq!(header.streams, vec!["btcusdt@trade"]);
    assert_eq!(header.symbols, vec!["BTCUSDT"]);

    // The file's shape: a trade, the drop, the retry, the outage gap, a trade.
    // The gap marker precedes the payload it describes, exactly as the live
    // channel emitted it.
    let shape: Vec<String> = records
        .iter()
        .map(|record| match record {
            exchange::Record::Data(data) => format!("data:{}", data.stream),
            exchange::Record::Marker(marker) => match &marker.marker {
                Marker::Gap(gap) => format!("gap:{:?}", gap.kind),
                Marker::Disconnect(_) => "disconnect".to_owned(),
                Marker::Reconnect(_) => "reconnect".to_owned(),
            },
        })
        .collect();
    assert_eq!(
        shape,
        vec![
            "data:btcusdt@trade",
            "disconnect",
            "reconnect",
            "gap:Outage",
            "data:btcusdt@trade",
        ]
    );

    // The reconnect marker proves a backoff was actually waited, not skipped.
    let Some(exchange::Record::Marker(reconnect)) = records.get(2) else {
        panic!("expected a reconnect marker");
    };
    let Marker::Reconnect(detail) = &reconnect.marker else {
        panic!("expected a reconnect marker");
    };
    assert_eq!(detail.attempt, 1);
    assert!(
        (8..=12).contains(&detail.waited_ms),
        "a 10ms base with +/-20% jitter: {}ms",
        detail.waited_ms
    );

    // The gap marker carries the policy that applied, so a replay judges it
    // under the same semantics rather than re-deriving them.
    let Some(exchange::Record::Marker(gap)) = records.get(3) else {
        panic!("expected a gap marker");
    };
    let Marker::Gap(detail) = &gap.marker else {
        panic!("expected a gap marker");
    };
    assert_eq!(detail.policy, exchange::SeqPolicy::Contiguous);
    assert_eq!(
        (detail.from, detail.to, detail.missing),
        (100, 105, Some(4))
    );

    // Replay: the recorded payloads, through the same pure normalizer, produce
    // the same events the live channel emitted.
    let replayed: Vec<MarketEvent> = records
        .iter()
        .filter_map(|record| match record {
            exchange::Record::Data(data) => Some(data.normalized().expect("replayable").event),
            exchange::Record::Marker(_) => None,
        })
        .collect();
    let live_events: Vec<MarketEvent> = live
        .into_iter()
        .filter_map(|message| match message {
            IngestMsg::Market(event) => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(replayed, live_events);

    // The ingest counter is monotonic across data and markers alike.
    let seqs: Vec<u64> = records.iter().map(exchange::Record::seq).collect();
    assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_payload_on_a_stream_we_never_subscribed_to_is_archived_but_not_emitted() {
    // Archived because the raw capture should hold whatever actually arrived;
    // not emitted because we cannot state the sequence semantics of a stream
    // nobody declared, and judging it under a guess is worse than dropping it.
    let dir = tempfile::tempdir().expect("a temp dir");
    let server = FakeServer::scripted(vec![
        Act::Ack,
        data(
            "ethusdt@trade",
            &trade("ETHUSDT", 1, 1_672_515_782_000, "10", "1"),
        ),
        data(
            "btcusdt@trade",
            &trade("BTCUSDT", 7, 1_672_515_782_100, "20", "1"),
        ),
        Act::Hold,
    ])
    .await;

    let (source, mut rx) = BinanceMarketSource::connect_with_clock(
        EndpointClass::Testnet,
        server.url(),
        SourceConfig {
            recording_dir: Some(dir.path().to_path_buf()),
            ..config(&[StreamKind::Trade])
        },
        Box::new(StepClock::new()),
    )
    .expect("a loopback URL is accepted under testnet");
    let path = source.recording_path().expect("recording").to_path_buf();
    let handle = tokio::spawn(source.run());

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    let MarketEvent::Trade(trade) = market(next(&mut rx).await) else {
        panic!("expected a trade");
    };
    assert_eq!(
        trade.symbol,
        symbol("BTCUSDT"),
        "the unsubscribed stream never reached the engine"
    );

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");

    let (_, records) = exchange::read_session(&path).expect("a readable recording");
    let streams: Vec<&str> = records
        .iter()
        .filter_map(|record| match record {
            exchange::Record::Data(data) => Some(data.stream.as_str()),
            exchange::Record::Marker(_) => None,
        })
        .collect();
    assert_eq!(streams, vec!["ethusdt@trade", "btcusdt@trade"]);
}

#[tokio::test]
async fn an_unreadable_payload_is_archived_and_dropped_rather_than_taking_the_feed_down() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let server = FakeServer::scripted(vec![
        Act::Ack,
        // Well-formed frame, unusable payload: the price is a JSON number, which
        // could only be read through a float.
        data(
            "btcusdt@trade",
            r#"{"e":"trade","E":1672515782000,"s":"BTCUSDT","t":1,"p":0.001,"q":"1","T":1672515782000,"m":true,"M":true}"#,
        ),
        data(
            "btcusdt@trade",
            &trade("BTCUSDT", 2, 1_672_515_782_100, "20", "1"),
        ),
        Act::Hold,
    ])
    .await;

    let (source, mut rx) = BinanceMarketSource::connect_with_clock(
        EndpointClass::Testnet,
        server.url(),
        SourceConfig {
            recording_dir: Some(dir.path().to_path_buf()),
            ..config(&[StreamKind::Trade])
        },
        Box::new(StepClock::new()),
    )
    .expect("a loopback URL is accepted under testnet");
    let path = source.recording_path().expect("recording").to_path_buf();
    let handle = tokio::spawn(source.run());

    assert_eq!(next(&mut rx).await, IngestMsg::Connected);
    let MarketEvent::Trade(trade) = market(next(&mut rx).await) else {
        panic!("expected a trade");
    };
    assert_eq!(
        trade.price,
        Price(decimal("20")),
        "only the readable one arrived"
    );

    drop(rx);
    handle.await.expect("joined").expect("a clean stop");

    // Both payloads are on disk. That is the point of recording raw: a later fix
    // to the normalizer re-derives the event we could not read today.
    let (_, records) = exchange::read_session(&path).expect("a readable recording");
    assert_eq!(records.len(), 2);
    assert!(matches!(records[0], exchange::Record::Data(_)));
}
