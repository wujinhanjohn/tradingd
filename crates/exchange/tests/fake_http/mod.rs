//! A scriptable HTTP server on `127.0.0.1:0`, for the REST tests.
//!
//! The same law as `fake_ws`: no test in this crate touches the live exchange.
//! A live endpoint is non-deterministic, rate-limited and occasionally down, and
//! a suite that leans on one rots and then gets ignored. The testnet is for the
//! manual end-to-end run only.
//!
//! The server is real - a real TCP listener speaking real HTTP/1.1 - so the
//! tests drive the real `RestClient`, the real `ureq` agent, and the real parse.
//! Only the exchange's behaviour is scripted, which is exactly the part a test
//! needs to control.
//!
//! It also **counts requests**, which is how "a mismatched endpoint sends
//! nothing" is proved rather than asserted: the count has to be zero.

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// One canned response.
#[derive(Clone, Debug)]
pub struct Reply {
    pub status: u16,
    pub reason: &'static str,
    pub body: String,
}

impl Reply {
    #[must_use]
    pub fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: body.into(),
        }
    }

    #[must_use]
    pub fn status(status: u16, reason: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            body: body.into(),
        }
    }
}

/// A running fake server. Dropping it shuts the listener down.
pub struct FakeRest {
    url: String,
    requests: Arc<AtomicUsize>,
    targets: Arc<Mutex<Vec<String>>>,
    _shutdown: mpsc::Sender<()>,
}

impl FakeRest {
    /// Start a server that answers the Nth request with the Nth reply.
    ///
    /// A request past the end of the list gets the last reply again, so a test
    /// about refreshes only has to describe the responses it cares about.
    pub async fn start(replies: Vec<Reply>) -> Self {
        assert!(!replies.is_empty(), "a fake server needs a reply to give");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback listener");
        let addr = listener.local_addr().expect("a bound address");
        let requests = Arc::new(AtomicUsize::new(0));
        let targets = Arc::new(Mutex::new(Vec::new()));

        // Held by the FakeRest; when it drops, the accept loop ends.
        let (shutdown, mut shutdown_rx) = mpsc::channel::<()>(1);

        {
            let requests = Arc::clone(&requests);
            let targets = Arc::clone(&targets);
            tokio::spawn(async move {
                loop {
                    let accepted = tokio::select! {
                        result = listener.accept() => result,
                        _ = shutdown_rx.recv() => return,
                    };
                    let Ok((mut stream, _)) = accepted else {
                        return;
                    };

                    // Read the request head. Every request here is a GET with no
                    // body, so the blank line that ends the headers ends it.
                    let mut head = Vec::new();
                    let mut byte = [0_u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match stream.read(&mut byte).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => head.push(byte[0]),
                        }
                    }
                    let text = String::from_utf8_lossy(&head).into_owned();
                    let target = text
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or_default()
                        .to_owned();

                    let index = requests.fetch_add(1, Ordering::SeqCst);
                    targets.lock().expect("targets lock").push(target);

                    let reply = replies
                        .get(index)
                        .or_else(|| replies.last())
                        .expect("a reply")
                        .clone();
                    let response = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: application/json;charset=UTF-8\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        reply.status,
                        reply.reason,
                        reply.body.len(),
                        reply.body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.flush().await;
                    let _ = stream.shutdown().await;
                }
            });
        }

        Self {
            url: format!("http://{addr}"),
            requests,
            targets,
            _shutdown: shutdown,
        }
    }

    /// The base URL to configure a client with, e.g. `http://127.0.0.1:53112`.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// How many requests have actually reached this server.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    /// The request targets seen so far, e.g. `/api/v3/exchangeInfo?symbol=BTCUSDT`.
    #[must_use]
    pub fn targets(&self) -> Vec<String> {
        self.targets.lock().expect("targets lock").clone()
    }
}
