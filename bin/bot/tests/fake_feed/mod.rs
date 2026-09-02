//! A minimal fake Binance combined-stream server, for the end-to-end tests.
//!
//! The binary tests run the *real* `bot` as a child process, so the feed it
//! connects to has to be a real server on a real port. Pointing it at the live
//! testnet instead would make `cargo test` depend on the network, on Binance
//! being up, and on rate limits - which is exactly the rot the milestone spec
//! rules out. The live testnet is for the manual demo only.
//!
//! Deliberately much smaller than the fake server in `exchange`'s own tests.
//! Those exercise the connection layer's behaviour; this one only has to be a
//! plausible feed, or a plausible refusal, while a whole process boots and shuts
//! down around it.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

/// What the fake exchange should do once a client subscribes.
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    /// Acknowledge, then push book tickers and trades until the client leaves.
    Feed,
    /// Refuse the subscription. The source treats this as fatal, which is how
    /// the tests kill the market source from the outside.
    Refuse,
}

/// A running fake feed. Dropping it stops the server thread.
pub struct FakeFeed {
    url: String,
    stop: Arc<Notify>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FakeFeed {
    /// Bind a loopback port and start serving.
    ///
    /// The listener is bound synchronously so the port is known before the
    /// server thread exists - the config file naming that port has to be written
    /// before the child process starts.
    pub fn start(mode: Mode) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("bound address").port();
        listener
            .set_nonblocking(true)
            .expect("tokio needs a non-blocking listener");

        let stop = Arc::new(Notify::new());
        let stop_thread = Arc::clone(&stop);

        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build a runtime for the fake feed");

            runtime.block_on(async move {
                let listener = TcpListener::from_std(listener).expect("adopt the listener");
                loop {
                    tokio::select! {
                        () = stop_thread.notified() => break,
                        accepted = listener.accept() => {
                            let Ok((stream, _)) = accepted else { break };
                            tokio::spawn(serve(stream, mode));
                        }
                    }
                }
            });
        });

        Self {
            // `/stream`: the combined endpoint, the same one the shipped config
            // points at, so the child exercises the production code path.
            url: format!("ws://127.0.0.1:{port}/stream"),
            stop,
            thread: Some(thread),
        }
    }

    /// The `ws://127.0.0.1:<port>/stream` URL to put in a config file.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for FakeFeed {
    fn drop(&mut self) {
        self.stop.notify_one();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

async fn serve(stream: TcpStream, mode: Mode) {
    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let (mut sink, mut source) = socket.split();

    // Wait for the SUBSCRIBE before answering it.
    if source.next().await.is_none() {
        return;
    }

    // Keep reading so pongs and the eventual close are consumed rather than
    // backing up in the socket.
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = source.next().await {} });

    match mode {
        Mode::Refuse => {
            let _ = sink
                .send(Message::text(
                    r#"{"id":1,"error":{"code":2,"msg":"Invalid request: unknown stream"}}"#
                        .to_owned(),
                ))
                .await;
        }
        Mode::Feed => {
            if sink
                .send(Message::text(r#"{"result":null,"id":1}"#.to_owned()))
                .await
                .is_err()
            {
                return;
            }

            // Contiguous trade ids and an order book updateId that leaps, which
            // is what the real streams do - so a run that logged a spurious gap
            // would show up here.
            let mut update_id: i64 = 400_900_000;
            let mut trade_id: i64 = 1;
            let mut event_ms: i64 = 1_712_345_678_000;

            loop {
                let book = format!(
                    r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{update_id},"s":"BTCUSDT","b":"64000.10000000","B":"0.50000000","a":"64000.20000000","A":"0.75000000"}}}}"#
                );
                let trade = format!(
                    r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","E":{event_ms},"s":"BTCUSDT","t":{trade_id},"p":"64000.15000000","q":"0.00100000","T":{event_ms},"m":true,"M":true}}}}"#
                );
                if sink.send(Message::text(book)).await.is_err()
                    || sink.send(Message::text(trade)).await.is_err()
                {
                    break;
                }
                update_id += 17;
                trade_id += 1;
                event_ms += 50;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }

    let _ = reader.await;
}
