//! A minimal fake `exchangeInfo` server, for the end-to-end tests.
//!
//! The binary tests run the *real* `bot` as a child process, and the real bot
//! now refuses to start until it has fetched symbol filters. So the tests need a
//! real HTTP server on a real port for the same reason they need a real
//! WebSocket one: pointing the child at the live testnet would make `cargo test`
//! depend on the network, on Binance being up, and on rate limits.
//!
//! Deliberately much smaller than the fake server in `exchange`'s own tests.
//! That one exercises the REST layer's behaviour; this one only has to be a
//! plausible exchange, or a plausible failure, while a whole process boots.
//!
//! Plain `std` sockets on a thread: one request, one response, no runtime.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// What the fake exchange should answer with.
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    /// A valid `exchangeInfo` response for BTCUSDT.
    Filters,
    /// A valid response that does not carry the configured symbol - the
    /// fail-closed case the bot must refuse to start on.
    WrongSymbol,
    /// A server-side failure. The bot must refuse to start, not run unfiltered.
    Unavailable,
}

/// The shape of a real response, trimmed to the fields the parse reads plus one
/// filter it deliberately does not model.
///
/// The verbatim capture lives in `crates/exchange/tests/fixtures/` and is what
/// the parsing tests run against; this is a stand-in for a process-level test
/// that only cares that the wiring works.
fn body(symbol: &str) -> String {
    format!(
        r#"{{"timezone":"UTC","serverTime":1788520355163,"symbols":[{{"symbol":"{symbol}",
        "status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
        {{"filterType":"PRICE_FILTER","minPrice":"0.01000000","maxPrice":"1000000.00000000",
          "tickSize":"0.01000000"}},
        {{"filterType":"LOT_SIZE","minQty":"0.00001000","maxQty":"9000.00000000",
          "stepSize":"0.00001000"}},
        {{"filterType":"MARKET_LOT_SIZE","minQty":"0.00000000","maxQty":"141.67845966",
          "stepSize":"0.00000000"}},
        {{"filterType":"NOTIONAL","minNotional":"5.00000000","applyMinToMarket":true,
          "maxNotional":"9000000.00000000","applyMaxToMarket":false,"avgPriceMins":5}},
        {{"filterType":"PERCENT_PRICE_BY_SIDE","bidMultiplierUp":"2","bidMultiplierDown":"0.5",
          "askMultiplierUp":"2","askMultiplierDown":"0.5","avgPriceMins":5}}
        ]}}]}}"#
    )
    .replace('\n', "")
    .replace("        ", "")
}

/// A running fake REST endpoint. Dropping it stops the server thread.
pub struct FakeRest {
    url: String,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FakeRest {
    /// Bind a loopback port and start serving.
    ///
    /// Bound synchronously so the port is known before the server thread exists:
    /// the config file naming that port has to be written before the child
    /// process starts.
    pub fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("bound address").port();
        listener
            .set_nonblocking(true)
            .expect("so the accept loop can notice the stop flag");

        let requests = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let thread = {
            let requests = Arc::clone(&requests);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            requests.fetch_add(1, Ordering::SeqCst);
                            serve(stream, mode);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })
        };

        Self {
            url: format!("http://127.0.0.1:{port}"),
            requests,
            stop,
            thread: Some(thread),
        }
    }

    /// The base URL to put in the config, e.g. `http://127.0.0.1:53112`.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// How many requests actually reached this server. Zero is a claim worth
    /// being able to make.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for FakeRest {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve(mut stream: TcpStream, mode: Mode) {
    // Read the request head: every request here is a GET with no body, so the
    // blank line that ends the headers ends it.
    let mut reader = BufReader::new(stream.try_clone().expect("clone the stream"));
    let mut line = String::new();
    while reader.read_line(&mut line).unwrap_or(0) > 0 {
        if line.ends_with("\r\n\r\n") || line == "\r\n" {
            break;
        }
        line.clear();
    }

    let (status, reason, payload) = match mode {
        Mode::Filters => (200, "OK", body("BTCUSDT")),
        Mode::WrongSymbol => (200, "OK", body("ETHUSDT")),
        Mode::Unavailable => (
            503,
            "Service Unavailable",
            r#"{"code":-1000,"msg":"An unknown error occurred while processing the request."}"#
                .to_owned(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json;charset=UTF-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}
