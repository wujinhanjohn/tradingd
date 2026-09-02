//! The socket itself: opening it, subscribing on it, and answering its pings.
//!
//! Everything above this module is pure or deterministic. This is the one place
//! that talks to a network, and it is kept as thin as it can be so that the
//! logic worth testing lives in [`crate::normalize`], [`crate::gap`],
//! [`crate::subscription`], [`crate::backoff`], and [`crate::wire`] instead.
//!
//! # Keepalive, verified rather than assumed
//!
//! Confirmed against the current spot docs: the server sends a **ping frame
//! every 20 seconds**, and if it does not get a **pong back within a minute** it
//! closes the connection. The pong should carry the ping's payload; an
//! unsolicited empty pong is permitted but explicitly does *not* count as
//! keepalive - so a naive "pong on a timer" design would be silently wrong and
//! would look fine right up until the disconnect.
//!
//! Tungstenite already does the right thing: `read` "will also queue responses
//! to ping and close messages", and "the next call to read, write or flush will
//! write & flush the pong reply. This means you should not respond to ping
//! frames manually." Queued is not the same as sent, though, and the sending is
//! conditional on us making another call. [`answer_ping`] therefore flushes
//! explicitly the moment a ping is seen, which is a no-op if the reply already
//! went out and a guarantee if it had not. That the pong actually reaches the
//! wire, with the ping's own payload, is pinned by a test against a local fake
//! server rather than taken on the library's word.
//!
//! # Connection lifetime
//!
//! Also confirmed: a single connection is only valid for **24 hours**, and a
//! `serverShutdown` notice precedes planned maintenance. Reconnection is
//! therefore a normal part of operation, not an error path - see
//! [`crate::source`].
//!
//! # TLS
//!
//! `wss://` needs a rustls crypto provider, and rustls 0.23 will not guess one:
//! if it cannot determine a process-level default it **panics** inside the
//! handshake, which surfaces as a dead task rather than a typed refusal.
//!
//! Two things prevent that here, and it is worth being precise about which does
//! the work. What actually resolves the provider is the crate feature: exactly
//! one provider feature (`ring`) is enabled on `rustls`, so rustls resolves it
//! from features alone. The compiler holds us to that - naming
//! `rustls::crypto::ring` below will not build without it - so the feature
//! cannot be dropped silently.
//!
//! [`install_crypto_provider`] is the second, weaker guarantee: it names the
//! choice in code, and it keeps working if a future dependency ever enables a
//! *second* provider, which would make feature-based resolution ambiguous and
//! bring the panic back.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::wire;

/// The live socket. `MaybeTlsStream` is what lets the same code path serve
/// `wss://` against the exchange and plain `ws://` against the fake server the
/// tests drive, so the tests exercise the real connection logic.
pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The id we put on our SUBSCRIBE request, and expect echoed back.
pub const SUBSCRIBE_REQUEST_ID: i64 = 1;

/// Failures opening or subscribing on a connection.
#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error("could not open a WebSocket to `{url}`")]
    Handshake {
        url: String,
        #[source]
        source: Box<tokio_tungstenite::tungstenite::Error>,
    },

    #[error("timed out after {}s opening a WebSocket to `{url}`", .timeout.as_secs())]
    HandshakeTimeout { url: String, timeout: Duration },

    #[error("could not send the SUBSCRIBE request for {} stream(s)", .streams)]
    Subscribe {
        streams: usize,
        #[source]
        source: Box<tokio_tungstenite::tungstenite::Error>,
    },

    #[error("could not build a SUBSCRIBE request")]
    RequestEncoding(#[source] serde_json::Error),
}

/// Install a rustls crypto provider before any `wss://` handshake.
///
/// Belt to the crate feature's braces - see the module docs for which does what.
/// This pins the choice to `ring` explicitly, so that a build which somehow ends
/// up with two provider features enabled gets a working default instead of a
/// panic inside the handshake.
///
/// Idempotent and tolerant: a provider already installed by the surrounding
/// process is left alone. `install_default` returns `Err` in that case, which is
/// a success for our purposes - a provider exists either way.
pub fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Open a connection and send the subscription request.
///
/// The acknowledgement is *not* awaited here. It is handled by the read loop in
/// [`crate::source`], alongside a deadline, so that a data frame arriving before
/// the ack is processed normally instead of being buffered or discarded by a
/// special-case handshake path.
///
/// # Errors
///
/// [`ConnectError`]. Every variant is a failed attempt that the caller retries
/// under backoff; none of them is fatal on its own.
pub async fn open(
    url: &str,
    streams: &[String],
    timeout: Duration,
) -> Result<Socket, ConnectError> {
    // Before the handshake: a `wss://` URL would otherwise panic mid-connect.
    install_crypto_provider();

    let request = wire::subscribe_request(SUBSCRIBE_REQUEST_ID, streams)
        .map_err(ConnectError::RequestEncoding)?;

    let connecting = connect_async(url);
    let (mut socket, _response) = tokio::time::timeout(timeout, connecting)
        .await
        .map_err(|_| ConnectError::HandshakeTimeout {
            url: url.to_owned(),
            timeout,
        })?
        .map_err(|source| ConnectError::Handshake {
            url: url.to_owned(),
            source: Box::new(source),
        })?;

    socket
        .send(Message::text(request))
        .await
        .map_err(|source| ConnectError::Subscribe {
            streams: streams.len(),
            source: Box::new(source),
        })?;

    Ok(socket)
}

/// Make sure the pong for a just-received ping actually leaves the machine.
///
/// Tungstenite has already queued the reply with the ping's payload; do not
/// write another one, because a custom pong would *replace* the queued reply
/// rather than accompany it. Flushing is the whole job.
///
/// # Errors
///
/// The underlying socket error, which the caller treats as a dropped connection.
pub async fn answer_ping(socket: &mut Socket) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    socket.flush().await
}

/// Read the next message, or `None` once the stream has ended.
///
/// A thin wrapper so the read loop does not need `StreamExt` in scope, and so
/// there is one obvious place to look for how the socket is driven.
pub async fn next_message(
    socket: &mut Socket,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    socket.next().await
}

/// Close the socket politely, ignoring the outcome.
///
/// Called on a clean shutdown. A failure here changes nothing - we are stopping
/// either way - so it is not worth a `Result` the caller would only discard.
pub async fn close(mut socket: Socket) {
    let _ = socket.close(None).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installing_the_crypto_provider_yields_a_usable_default() {
        // Narrow on purpose: this pins the explicit-install half only. rustls'
        // feature-based resolution is private, so no test can call it - the
        // end-to-end guarantee that a `wss://` handshake does not panic lives in
        // `tests/connection.rs`, which drives a real TLS connect.
        install_crypto_provider();
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "no rustls crypto provider: a wss:// handshake would panic"
        );
        // Idempotent: a second call must not panic or clobber the first.
        install_crypto_provider();
    }
}
