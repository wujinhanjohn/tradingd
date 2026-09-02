//! A scriptable WebSocket server on `127.0.0.1:0`, for the connection tests.
//!
//! Every socket test in this crate runs against this, never against the live
//! testnet. A live feed is non-deterministic, rate-limited, and occasionally
//! down; a suite that leans on one rots, and then gets ignored. The testnet is
//! for the manual end-to-end demo only.
//!
//! The server is real - a real TCP listener, a real WebSocket handshake, real
//! frames - so the tests drive the real `BinanceMarketSource::connect`, the real
//! subscribe path, and tungstenite's real ping handling. Nothing is stubbed
//! except the exchange's behaviour, which is exactly the thing a test needs to
//! control.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// One thing the server should do on a connection, in order.
#[derive(Clone, Debug)]
pub enum Act {
    /// Send a text frame verbatim.
    Text(String),
    /// Send the standard `{"result":null,"id":1}` acknowledgement.
    Ack,
    /// Send a WebSocket ping frame carrying this payload.
    Ping(&'static [u8]),
    /// Send a close frame and hang up politely.
    Close,
    /// Drop the TCP connection with no close handshake, the way a network
    /// failure or a forced server-side disconnect looks.
    Drop,
    /// Wait for the client to have sent at least this many pongs.
    AwaitPongs(usize),
    /// Stay connected and silent until the client goes away.
    Hold,
}

/// What the server saw a client do.
#[derive(Clone, Debug, Default)]
pub struct Observed {
    /// Text frames the client sent - in practice, the SUBSCRIBE requests.
    pub requests: Vec<String>,
    /// Payloads of the pong frames the client sent back.
    pub pongs: Vec<Vec<u8>>,
}

/// A running fake server. Dropping it shuts the listener down.
pub struct FakeServer {
    url: String,
    observed: Arc<Mutex<Vec<Observed>>>,
    connections: Arc<Mutex<usize>>,
    _shutdown: mpsc::Sender<()>,
    accepted_rx: Arc<tokio::sync::Notify>,
}

impl FakeServer {
    /// Start a server whose Nth connection follows the Nth script.
    ///
    /// A connection past the end of the scripts is held open and silent, so a
    /// test that only cares about the first two connections does not have to
    /// describe every retry that might follow.
    pub async fn start(scripts: Vec<Vec<Act>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback listener");
        let addr = listener.local_addr().expect("a bound address");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(Mutex::new(0usize));
        let notify = Arc::new(tokio::sync::Notify::new());

        // Held by the FakeServer; when it drops, the accept loop ends.
        let (shutdown, mut shutdown_rx) = mpsc::channel::<()>(1);

        {
            let observed = Arc::clone(&observed);
            let connections = Arc::clone(&connections);
            let notify = Arc::clone(&notify);
            tokio::spawn(async move {
                let mut index = 0usize;
                loop {
                    let accepted = tokio::select! {
                        result = listener.accept() => result,
                        _ = shutdown_rx.recv() => return,
                    };
                    let Ok((stream, _)) = accepted else { return };

                    let script = scripts
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| vec![Act::Hold]);
                    index += 1;
                    *connections.lock().expect("connection count") = index;
                    notify.notify_waiters();

                    let observed = Arc::clone(&observed);
                    tokio::spawn(async move {
                        serve(stream, script, observed).await;
                    });
                }
            });
        }

        Self {
            // `/stream`, the combined endpoint: the same path the real source
            // uses, so the wrapper handling under test is the production one.
            url: format!("ws://127.0.0.1:{}/stream", addr.port()),
            observed,
            connections,
            _shutdown: shutdown,
            accepted_rx: notify,
        }
    }

    /// A single-connection server.
    pub async fn scripted(script: Vec<Act>) -> Self {
        Self::start(vec![script]).await
    }

    /// The `ws://127.0.0.1:<port>/stream` URL to point a source at.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// How many connections have been accepted so far.
    pub fn connection_count(&self) -> usize {
        *self.connections.lock().expect("connection count")
    }

    /// What the Nth connection observed, if it has been accepted.
    pub fn observed(&self, index: usize) -> Option<Observed> {
        self.observed
            .lock()
            .expect("observations")
            .get(index)
            .cloned()
    }

    /// Block until `count` connections have been accepted.
    pub async fn await_connections(&self, count: usize, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.connection_count() < count {
            let notified = self.accepted_rx.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                panic!(
                    "only {} of {count} connections were accepted in time",
                    self.connection_count()
                );
            }
        }
    }

    /// Block until the Nth connection has recorded `count` text requests, and
    /// return them.
    ///
    /// The server logs what it reads on a separate task, so "the client has sent
    /// it" and "the server has recorded it" are different instants. Polling here
    /// is what keeps that from being a race in the test.
    pub async fn await_requests(
        &self,
        index: usize,
        count: usize,
        timeout: Duration,
    ) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(observed) = self.observed(index) {
                if observed.requests.len() >= count {
                    return observed.requests;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "connection {index} sent {} requests, expected {count}",
                self.observed(index).map_or(0, |o| o.requests.len())
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Block until the Nth connection has recorded `count` pongs, and return
    /// them.
    pub async fn await_pongs(&self, index: usize, count: usize, timeout: Duration) -> Vec<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(observed) = self.observed(index) {
                if observed.pongs.len() >= count {
                    return observed.pongs;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "connection {index} sent {} pongs, expected {count}",
                self.observed(index).map_or(0, |o| o.pongs.len())
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

async fn serve(stream: TcpStream, script: Vec<Act>, log: Arc<Mutex<Vec<Observed>>>) {
    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };

    let index = {
        let mut log = log.lock().expect("observations");
        log.push(Observed::default());
        log.len() - 1
    };

    let (mut sink, mut source) = socket.split();

    // Reading and writing run concurrently: the script may need to send a ping
    // and then wait for the pong that answers it.
    let reader_log = Arc::clone(&log);
    let reader = tokio::spawn(async move {
        while let Some(Ok(message)) = source.next().await {
            let mut log = reader_log.lock().expect("observations");
            let observed = &mut log[index];
            match message {
                Message::Text(text) => observed.requests.push(text.to_string()),
                Message::Pong(payload) => observed.pongs.push(payload.to_vec()),
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    for act in script {
        let sent = match act {
            Act::Text(text) => sink.send(Message::text(text)).await,
            Act::Ack => {
                sink.send(Message::text(r#"{"result":null,"id":1}"#.to_owned()))
                    .await
            }
            Act::Ping(payload) => sink.send(Message::Ping(payload.into())).await,
            Act::Close => {
                let _ = sink.send(Message::Close(None)).await;
                break;
            }
            Act::Drop => break,
            Act::AwaitPongs(want) => {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                loop {
                    let seen = log.lock().expect("observations")[index].pongs.len();
                    if seen >= want || tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Ok(())
            }
            Act::Hold => {
                // Stay connected and silent. This is what a stream that has gone
                // quiet looks like from the client's side, and it is also the
                // default for a connection the test did not script.
                let _ = reader.await;
                return;
            }
        };
        if sent.is_err() {
            break;
        }
    }

    // `Drop` and the end of a script both land here: dropping both halves closes
    // the socket without a close handshake, which is what an abrupt
    // server-side disconnect looks like on the wire.
    reader.abort();
    drop(sink);
    let _ = reader.await;
}

// --- frame builders, spelled the way Binance spells them ---

/// A combined-stream data frame: `{"stream":..,"data":..}`.
pub fn data(stream: &str, payload: &str) -> Act {
    Act::Text(format!(r#"{{"stream":"{stream}","data":{payload}}}"#))
}

/// A `<symbol>@bookTicker` payload. Prices and quantities are JSON strings,
/// exactly as the exchange sends them.
pub fn book_ticker(symbol: &str, update_id: i64, bid: &str, ask: &str) -> String {
    format!(
        r#"{{"u":{update_id},"s":"{symbol}","b":"{bid}","B":"1.00000000","a":"{ask}","A":"2.00000000"}}"#
    )
}

/// A `<symbol>@trade` payload.
pub fn trade(symbol: &str, trade_id: i64, event_ms: i64, price: &str, qty: &str) -> String {
    format!(
        r#"{{"e":"trade","E":{event_ms},"s":"{symbol}","t":{trade_id},"p":"{price}","q":"{qty}","T":{event_ms},"m":true,"M":true}}"#
    )
}
