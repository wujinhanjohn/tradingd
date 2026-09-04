//! Canonical Binance hosts, and the structural check that stops a
//! testnet-labelled configuration from ever reaching a production host.
//!
//! This is the milestone-1 backlog item ("environment and endpoint are not
//! cross-checked"). It could not be fixed in `settings`, because knowing which
//! hostname is the real exchange is Binance-specific knowledge, and it belongs
//! here.
//!
//! The check is *structural*: `BinanceMarketSource::connect` takes the expected
//! [`EndpointClass`] and calls [`require_class`] before it opens a socket, and
//! `RestClient::new` calls [`require_class_for`] before it can issue a request,
//! so a client aimed at the wrong environment cannot be constructed at all.
//! There is no "warn and continue" path.
//!
//! Matching is **exact, on the host, and case-insensitive ASCII only**. It is
//! never a suffix or substring test. `data-stream.binance.vision` is the reason
//! why: it shares the `binance.vision` suffix with every testnet host but serves
//! live production market data, so a `ends_with("binance.vision")` shortcut
//! would classify the live feed as testnet. Anything not on a list below is
//! unrecognised, and unrecognised fails closed.
//!
//! # Two protocols, one parser
//!
//! Milestone 3 added the first REST call. A REST base URL is another endpoint
//! and goes through *this* module - the same authority parser, the same host
//! lists - rather than a second validator written next to the HTTP client. All
//! [`Protocol`] changes is which two schemes are accepted:
//!
//! | protocol | secure (required against a real host) | plaintext (loopback, testnet only) |
//! |---|---|---|
//! | [`Protocol::WebSocket`] | `wss://` | `ws://` |
//! | [`Protocol::Rest`]      | `https://` | `http://` |
//!
//! A second parser would be the bug: two parsers that disagree about where a
//! host ends is exactly the confusion [`host_of`] exists to refuse.

/// Which exchange environment a URL actually points at.
///
/// Serialisable because it is stamped into every session recording's header: a
/// capture must say for itself whether it came from testnet or production, since
/// a filename does not survive being moved and a URL can be re-read wrongly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointClass {
    Testnet,
    Production,
}

/// Which wire protocol an endpoint URL is for.
///
/// This selects the scheme pair and nothing else. Classification keys on the
/// host, exactly as it always has, so the two protocols share one set of host
/// lists and one authority parser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// Market and user data streams: `wss://`, or `ws://` on loopback.
    WebSocket,
    /// The REST API: `https://`, or `http://` on loopback.
    Rest,
}

/// Hosts serving the Binance **spot testnet**. No real funds.
///
/// Both protocols in one list, because classification is a question about the
/// *host*. `testnet.binance.vision` is the REST host, confirmed against the
/// testnet landing page during milestone 3 (`https://testnet.binance.vision/api`
/// is the documented REST base); the `stream.` and `ws-api.` hosts serve the
/// sockets.
pub const TESTNET_HOSTS: &[&str] = &[
    "stream.testnet.binance.vision",
    "ws-api.testnet.binance.vision",
    "testnet.binance.vision",
];

/// Hosts serving **live, real-money** Binance spot.
///
/// `data-stream.binance.vision` is market-data-only, but it is *production*
/// market data. It is listed here deliberately: it is the host most likely to
/// be mistaken for a testnet endpoint. `data-api.binance.vision` is its REST
/// twin and is listed for the same reason.
///
/// The `api1`-`api4` and `api-gcp` hosts are documented production alternatives
/// ("better performance but less stability"). They are listed because an
/// operator may legitimately configure one, and a production host that is
/// *missing* from this list classifies as `None` - a refusal, which is safe but
/// unhelpful. A production host wrongly placed in [`TESTNET_HOSTS`] would be
/// catastrophic and silent, which is why the rule is: when unsure, leave a host
/// off both lists entirely.
///
/// NOTE: that classification is a documented fact about Binance, not something
/// the tests below can establish - they only assert what we wrote down. It is on
/// the milestone verify list. If Binance repurposes a host, this constant is
/// what has to change.
pub const PRODUCTION_HOSTS: &[&str] = &[
    "stream.binance.com",
    "ws-api.binance.com",
    "data-stream.binance.vision",
    "api.binance.com",
    "api-gcp.binance.com",
    "api1.binance.com",
    "api2.binance.com",
    "api3.binance.com",
    "api4.binance.com",
    "data-api.binance.vision",
];

/// The canonical testnet spot market-stream URL. This is what a testnet config
/// should say.
///
/// `/stream`, the **combined** endpoint, not `/ws`. Only the combined endpoint
/// wraps each payload as `{"stream":..,"data":..}`, and that name is what
/// [`crate::normalize`] cross-checks the payload against, what [`crate::gap`]
/// keys on, and what makes a recording replayable. Classification itself keys on
/// the host, so the path here is a statement about which endpoint to *use*, not
/// part of what makes a URL testnet.
pub const TESTNET_SPOT_WS_URL: &str = "wss://stream.testnet.binance.vision/stream";

/// The canonical production spot market-stream URL, recorded so the classifier
/// has something to be tested against. Nothing in this milestone connects to it.
pub const PRODUCTION_SPOT_WS_URL: &str = "wss://stream.binance.com:9443/stream";

/// The canonical testnet spot REST base URL. The `/api/v3/...` path is appended
/// by [`crate::rest`]; this constant is the base an operator configures.
pub const TESTNET_SPOT_REST_URL: &str = "https://testnet.binance.vision";

/// The canonical production spot REST base URL, recorded so the classifier has
/// something to be tested against. Nothing in this milestone connects to it.
pub const PRODUCTION_SPOT_REST_URL: &str = "https://api.binance.com";

/// Loopback authorities, which by construction cannot be an exchange.
///
/// These exist for the local fake servers the tests run against - a WebSocket
/// server for the connection tests, an HTTP server for the REST tests - so those
/// tests exercise the *real* constructor rather than an unchecked back door. See
/// [`require_class_for`] for the exact, narrow allowance.
const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "localhost", "[::1]"];

/// A refusal to connect. Every variant means "did not open a socket, did not
/// send a request".
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum EndpointError {
    #[error(
        "refusing to connect: configuration says `{expected}` but `{url}` is a \
         `{found}` endpoint. Point the URL at {expected} hosts, or change the \
         configured environment - never both-and"
    )]
    Mismatch {
        expected: EndpointClass,
        found: EndpointClass,
        url: String,
    },

    #[error(
        "refusing to connect: `{url}` is not a recognised Binance `{expected}` \
         {} endpoint. Expected {} against one of: {}",
        .protocol.as_str(),
        .protocol.secure_scheme(),
        .expected.hosts().join(", ")
    )]
    Unrecognised {
        expected: EndpointClass,
        protocol: Protocol,
        url: String,
    },
}

impl EndpointClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Testnet => "testnet",
            Self::Production => "production",
        }
    }

    /// The canonical hosts for this class, across both protocols.
    #[must_use]
    pub fn hosts(self) -> &'static [&'static str] {
        match self {
            Self::Testnet => TESTNET_HOSTS,
            Self::Production => PRODUCTION_HOSTS,
        }
    }

    #[must_use]
    pub fn is_production(self) -> bool {
        matches!(self, Self::Production)
    }
}

impl std::fmt::Display for EndpointClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Protocol {
    /// The scheme required against a real exchange host. TLS is not optional.
    #[must_use]
    pub const fn secure_scheme(self) -> &'static str {
        match self {
            Self::WebSocket => "wss://",
            Self::Rest => "https://",
        }
    }

    /// The plaintext scheme, accepted **only** on loopback and **only** in the
    /// testnet direction. See [`require_class_for`].
    #[must_use]
    pub const fn plaintext_scheme(self) -> &'static str {
        match self {
            Self::WebSocket => "ws://",
            Self::Rest => "http://",
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WebSocket => "WebSocket",
            Self::Rest => "REST",
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Classify a WebSocket URL by its host. [`classify_for`] with
/// [`Protocol::WebSocket`].
#[must_use]
pub fn classify(url: &str) -> Option<EndpointClass> {
    classify_for(Protocol::WebSocket, url)
}

/// Classify a URL by its host.
///
/// Returns `None` for anything that is not exactly one of the canonical hosts
/// over `protocol`'s secure scheme - including loopback, unknown hosts,
/// plaintext against a real host, the *other* protocol's scheme, and any URL
/// whose authority we cannot parse unambiguously. `None` is not "probably fine";
/// callers must treat it as a refusal.
#[must_use]
pub fn classify_for(protocol: Protocol, url: &str) -> Option<EndpointClass> {
    // TLS is not optional against the real exchange. A `ws://` or `http://` URL
    // naming a Binance host is either a typo or an attempted downgrade; either
    // way it is not something we will classify as a valid endpoint.
    let host = host_of(url.strip_prefix(protocol.secure_scheme())?)?;

    if TESTNET_HOSTS.iter().any(|h| host.eq_ignore_ascii_case(h)) {
        return Some(EndpointClass::Testnet);
    }
    if PRODUCTION_HOSTS
        .iter()
        .any(|h| host.eq_ignore_ascii_case(h))
    {
        return Some(EndpointClass::Production);
    }
    None
}

/// Whether `url` names a loopback address, over either WebSocket scheme.
/// [`is_loopback_for`] with [`Protocol::WebSocket`].
#[must_use]
pub fn is_loopback(url: &str) -> bool {
    is_loopback_for(Protocol::WebSocket, url)
}

/// Whether `url` names a loopback address.
///
/// True only for the literal loopback authorities, over either of `protocol`'s
/// schemes. A loopback address can never be Binance, which is what makes the
/// allowance in [`require_class_for`] safe.
#[must_use]
pub fn is_loopback_for(protocol: Protocol, url: &str) -> bool {
    let rest = url
        .strip_prefix(protocol.secure_scheme())
        .or_else(|| url.strip_prefix(protocol.plaintext_scheme()));
    rest.and_then(host_of)
        .is_some_and(|host| LOOPBACK_HOSTS.iter().any(|h| host.eq_ignore_ascii_case(h)))
}

/// The gate, for a WebSocket URL. [`require_class_for`] with
/// [`Protocol::WebSocket`].
///
/// # Errors
///
/// As [`require_class_for`].
pub fn require_class(expected: EndpointClass, url: &str) -> Result<(), EndpointError> {
    require_class_for(Protocol::WebSocket, expected, url)
}

/// The gate. Refuse unless `url` provably belongs to the `expected` environment.
///
/// The one carve-out is deliberate and narrow: a loopback URL is accepted when
/// `expected` is [`EndpointClass::Testnet`], so the tests drive the real
/// constructors against local fake servers. Loopback under
/// [`EndpointClass::Production`] is refused like anything else - the carve-out
/// only ever makes the *safe* direction more permissive, and it cannot put a
/// production-labelled run anywhere near a socket or a request that is not a
/// real one.
///
/// # Errors
///
/// [`EndpointError::Mismatch`] when the URL is a recognised endpoint for the
/// other environment - the dangerous case, worth its own message - and
/// [`EndpointError::Unrecognised`] when it is not a recognised endpoint at all.
pub fn require_class_for(
    protocol: Protocol,
    expected: EndpointClass,
    url: &str,
) -> Result<(), EndpointError> {
    match classify_for(protocol, url) {
        Some(found) if found == expected => Ok(()),
        Some(found) => Err(EndpointError::Mismatch {
            expected,
            found,
            url: url.to_owned(),
        }),
        None if expected == EndpointClass::Testnet && is_loopback_for(protocol, url) => Ok(()),
        None => Err(EndpointError::Unrecognised {
            expected,
            protocol,
            url: url.to_owned(),
        }),
    }
}

/// Extract the host from a URL authority, or `None` if it is in any way
/// ambiguous.
///
/// Hand-written rather than delegating to a URL crate, because this is a
/// security check and the failure mode that matters is *accepting* something we
/// should not have. Everything unusual returns `None`, which callers turn into a
/// refusal, so the parser is allowed to be strict to the point of pedantry.
///
/// `after_scheme` is the URL with its `ws://`/`wss://` prefix already stripped.
fn host_of(after_scheme: &str) -> Option<&str> {
    // The authority ends at the first path, query, or fragment delimiter.
    let end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = after_scheme.get(..end)?;

    // Allow-list, not deny-list. A deny-list has to anticipate every way a host
    // can be disguised; an allow-list only has to describe the handful of
    // characters a real Binance authority contains, and everything else - which
    // is where hand-rolled parsers hide their bugs - refuses by default:
    //
    // - `@` userinfo: `wss://stream.testnet.binance.vision@evil.example/ws`
    //   reads left-to-right as a testnet host and connects to `evil.example`.
    // - `%` percent-encoding: `testnet%2ebinance.vision` decodes to a listed
    //   host in a decoding parser. We never decode, and now never accept it.
    // - `\` backslash: WHATWG URL parsing treats it as a path separator, so a
    //   backslash is a way to make two parsers disagree about where the host ends.
    // - Control characters and whitespace: WHATWG strips tab/CR/LF *before*
    //   parsing, so `stream.binance.co\nm` is one host to a browser and another
    //   to us. Disagreement in either direction is a reason to refuse.
    // - Non-ASCII: homoglyph and punycode confusables never reach a comparison.
    if authority.is_empty() || !authority.bytes().all(is_authority_byte) {
        return None;
    }

    if authority.starts_with('[') {
        // Bracketed IPv6 literal. The host includes the brackets; only a port
        // may follow. No canonical Binance host is an IP, so this exists to be
        // parsed unambiguously and then not match anything.
        let close = authority.find(']')?;
        let (host, rest) = authority.split_at(close + 1);
        if rest.is_empty() || rest.starts_with(':') {
            Some(host)
        } else {
            None
        }
    } else {
        let mut parts = authority.split(':');
        let host = parts.next().filter(|h| !h.is_empty())?;
        // host, then at most a port. A second colon means a bare IPv6 literal or
        // a malformed authority; either way we decline to guess.
        let _port = parts.next();
        if parts.next().is_some() {
            return None;
        }
        Some(host)
    }
}

/// The only bytes that may appear in an authority we are willing to classify.
///
/// Deliberately narrower than the URL spec: no `_`, no userinfo, no encoding.
/// Nothing Binance serves needs more than this.
const fn is_authority_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_constants_classify_as_themselves() {
        // If someone edits a constant, this is what notices.
        assert_eq!(
            classify(TESTNET_SPOT_WS_URL),
            Some(EndpointClass::Testnet),
            "{TESTNET_SPOT_WS_URL}"
        );
        assert_eq!(
            classify(PRODUCTION_SPOT_WS_URL),
            Some(EndpointClass::Production),
            "{PRODUCTION_SPOT_WS_URL}"
        );
    }

    #[test]
    fn every_listed_host_classifies_into_its_own_list() {
        for host in TESTNET_HOSTS {
            let url = format!("wss://{host}/ws");
            assert_eq!(classify(&url), Some(EndpointClass::Testnet), "{url}");
        }
        for host in PRODUCTION_HOSTS {
            let url = format!("wss://{host}/ws");
            assert_eq!(classify(&url), Some(EndpointClass::Production), "{url}");
        }
    }

    #[test]
    fn the_two_host_lists_never_overlap() {
        for t in TESTNET_HOSTS {
            assert!(
                !PRODUCTION_HOSTS.contains(t),
                "`{t}` is listed as both testnet and production"
            );
        }
    }

    #[test]
    fn data_stream_binance_vision_is_production_not_testnet() {
        // The suffix trap. `binance.vision` is shared with every testnet host,
        // but this one carries live market data.
        assert_eq!(
            classify("wss://data-stream.binance.vision/ws/btcusdt@trade"),
            Some(EndpointClass::Production)
        );
    }

    #[test]
    fn classification_ignores_port_path_and_query() {
        for url in [
            "wss://stream.binance.com:9443/ws",
            "wss://stream.binance.com:443/ws/btcusdt@bookTicker",
            "wss://stream.binance.com/stream?streams=btcusdt@trade/ethusdt@trade",
            "wss://stream.binance.com",
            "wss://stream.binance.com/",
            "wss://stream.binance.com#frag",
        ] {
            assert_eq!(classify(url), Some(EndpointClass::Production), "{url}");
        }
    }

    #[test]
    fn the_combined_stream_path_classifies_exactly_as_the_raw_one_does() {
        // Milestone 2 moved the shipped config from `/ws` to `/stream`, because
        // only the combined endpoint names the stream each payload belongs to.
        // Classification keys on the *host*, so that move must be invisible
        // here - including for the two mismatch directions, which are the
        // reason this module exists.
        for host in TESTNET_HOSTS {
            for path in ["/ws", "/stream", "/stream?streams=btcusdt@trade"] {
                let url = format!("wss://{host}{path}");
                assert_eq!(classify(&url), Some(EndpointClass::Testnet), "{url}");
                assert_eq!(require_class(EndpointClass::Testnet, &url), Ok(()), "{url}");
                assert!(
                    matches!(
                        require_class(EndpointClass::Production, &url),
                        Err(EndpointError::Mismatch { .. })
                    ),
                    "{url} must still refuse under a production label"
                );
            }
        }

        for host in PRODUCTION_HOSTS {
            for path in ["/ws", "/stream", "/stream?streams=btcusdt@trade"] {
                let url = format!("wss://{host}{path}");
                assert_eq!(classify(&url), Some(EndpointClass::Production), "{url}");
                // The direction that must never regress, path or no path.
                assert!(
                    matches!(
                        require_class(EndpointClass::Testnet, &url),
                        Err(EndpointError::Mismatch { .. })
                    ),
                    "{url} must refuse under a testnet label"
                );
            }
        }
    }

    #[test]
    fn hostnames_are_matched_case_insensitively() {
        assert_eq!(
            classify("wss://Stream.TestNet.Binance.Vision/ws"),
            Some(EndpointClass::Testnet)
        );
    }

    #[test]
    fn a_host_that_merely_contains_a_canonical_name_is_not_recognised() {
        for url in [
            // Suffix attack: canonical name as a subdomain of somewhere else.
            "wss://stream.testnet.binance.vision.evil.example/ws",
            "wss://stream.binance.com.evil.example/ws",
            // Prefix attack.
            "wss://notstream.binance.com/ws",
            "wss://xstream.testnet.binance.vision/ws",
            // Canonical name in the path, not the host.
            "wss://evil.example/stream.testnet.binance.vision/ws",
            // Canonical name in a query parameter.
            "wss://evil.example/ws?host=stream.binance.com",
            // Bare registrable domain, which serves nothing we subscribe to.
            "wss://binance.vision/ws",
            "wss://binance.com/ws",
            // Trailing dot: technically the same host, but we decline to guess.
            "wss://stream.binance.com./ws",
        ] {
            assert_eq!(classify(url), None, "{url} must not be recognised");
        }
    }

    #[test]
    fn userinfo_host_confusion_is_refused() {
        // Reads left-to-right as a testnet host; actually connects to evil.example.
        for url in [
            "wss://stream.testnet.binance.vision@evil.example/ws",
            "wss://user:pass@stream.binance.com/ws",
            "wss://stream.binance.com@evil.example:443/ws",
        ] {
            assert_eq!(classify(url), None, "{url} must not be recognised");
            // And the gate refuses it in both directions.
            assert!(require_class(EndpointClass::Testnet, url).is_err(), "{url}");
            assert!(
                require_class(EndpointClass::Production, url).is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn plaintext_ws_against_a_real_host_is_not_recognised() {
        // TLS is not optional against the exchange.
        assert_eq!(classify("ws://stream.binance.com:9443/ws"), None);
        assert_eq!(classify("ws://stream.testnet.binance.vision/ws"), None);
    }

    #[test]
    fn non_websocket_schemes_are_not_recognised() {
        for url in [
            "https://stream.binance.com/ws",
            "http://stream.binance.com/ws",
            "stream.binance.com/ws",
            "//stream.binance.com/ws",
            "WSS://stream.binance.com/ws",
        ] {
            assert_eq!(classify(url), None, "{url}");
        }
    }

    #[test]
    fn malformed_and_empty_urls_are_not_recognised() {
        for url in [
            "",
            "wss://",
            "wss:///ws",
            "wss://:443/ws",
            "wss://a:b:c/ws",
            "wss://[::1/ws",
            "wss://[::1]x/ws",
            "not a url at all",
        ] {
            assert_eq!(classify(url), None, "{url}");
        }
    }

    #[test]
    fn matching_directions_are_accepted() {
        assert!(require_class(EndpointClass::Testnet, TESTNET_SPOT_WS_URL).is_ok());
        assert!(require_class(EndpointClass::Production, PRODUCTION_SPOT_WS_URL).is_ok());
    }

    #[test]
    fn testnet_config_aimed_at_a_production_host_refuses() {
        // THE test. This is the failure that passes the M1 environment guard and
        // then trades real funds. It must never start.
        for url in [
            PRODUCTION_SPOT_WS_URL,
            "wss://stream.binance.com:443/ws",
            "wss://data-stream.binance.vision/ws",
            "wss://ws-api.binance.com/ws-api/v3",
        ] {
            let err = require_class(EndpointClass::Testnet, url)
                .expect_err("a testnet config must never reach a production host");
            assert_eq!(
                err,
                EndpointError::Mismatch {
                    expected: EndpointClass::Testnet,
                    found: EndpointClass::Production,
                    url: url.to_owned(),
                },
                "{url}"
            );
            let msg = err.to_string();
            assert!(msg.contains("refusing to connect"), "{msg}");
            assert!(msg.contains(url), "message must name the URL: {msg}");
        }
    }

    #[test]
    fn production_config_aimed_at_a_testnet_host_refuses() {
        // The other direction: less dangerous, equally wrong. A production run
        // silently pointed at testnet reports fills that never happened.
        for url in [
            TESTNET_SPOT_WS_URL,
            "wss://ws-api.testnet.binance.vision/ws",
        ] {
            let err = require_class(EndpointClass::Production, url)
                .expect_err("must refuse the mismatch");
            assert_eq!(
                err,
                EndpointError::Mismatch {
                    expected: EndpointClass::Production,
                    found: EndpointClass::Testnet,
                    url: url.to_owned(),
                },
                "{url}"
            );
        }
    }

    #[test]
    fn an_unrecognised_host_refuses_for_both_expectations() {
        let url = "wss://stream.binance.example/ws";
        for expected in [EndpointClass::Testnet, EndpointClass::Production] {
            let err = require_class(expected, url).expect_err("must refuse the unknown");
            assert_eq!(
                err,
                EndpointError::Unrecognised {
                    expected,
                    protocol: Protocol::WebSocket,
                    url: url.to_owned(),
                }
            );
            // The refusal must tell the operator what would have been accepted.
            let msg = err.to_string();
            for host in expected.hosts() {
                assert!(msg.contains(host), "message should list `{host}`: {msg}");
            }
        }
    }

    #[test]
    fn loopback_is_recognised_only_as_loopback() {
        for url in [
            "ws://127.0.0.1:8080/ws",
            "ws://localhost/ws",
            "wss://127.0.0.1:0/ws",
            "ws://[::1]:9001/ws",
            "ws://LOCALHOST/ws",
        ] {
            assert!(is_loopback(url), "{url}");
            // Never classified as a real environment.
            assert_eq!(classify(url), None, "{url}");
        }
        for url in [
            TESTNET_SPOT_WS_URL,
            PRODUCTION_SPOT_WS_URL,
            "ws://127.0.0.1.evil.example/ws",
            "ws://localhost.evil.example/ws",
            "ws://192.168.1.10/ws",
            "",
        ] {
            assert!(!is_loopback(url), "{url} must not be loopback");
        }
    }

    #[test]
    fn loopback_is_allowed_only_in_the_testnet_direction() {
        // The narrow carve-out that lets the connection tests drive the real
        // constructor against a local fake server.
        //
        // Red if deleted: a production run must never accept a local or fake
        // market feed. Orders would route to the real exchange while prices came
        // from a socket on this machine - the worst possible disagreement between
        // what the bot believes and what it is doing.
        for host in [
            "127.0.0.1:8080",
            "localhost",
            "[::1]:9001",
            "127.0.0.1",
            "LocalHost:443",
        ] {
            for scheme in ["ws", "wss"] {
                let url = format!("{scheme}://{host}/ws");

                assert_eq!(
                    require_class(EndpointClass::Testnet, &url),
                    Ok(()),
                    "testnet must still reach a local fake server: {url}"
                );

                assert_eq!(
                    require_class(EndpointClass::Production, &url),
                    Err(EndpointError::Unrecognised {
                        expected: EndpointClass::Production,
                        protocol: Protocol::WebSocket,
                        url: url.clone(),
                    }),
                    "production must refuse a loopback feed: {url}"
                );
            }
        }
    }

    #[test]
    fn the_parser_itself_extracts_no_host_from_a_hostile_authority() {
        // `classify` would refuse these anyway, because none of them spell a
        // listed host without a decoding step we never perform. That makes the
        // refusal tests above pass even with a sloppy parser, so this test pins
        // the parser instead of the outcome: `host_of` must decline to extract
        // *anything*, so no future caller can be handed a half-parsed authority.
        for authority in [
            // userinfo
            "stream.testnet.binance.vision@evil.example",
            "user:pass@stream.binance.com",
            // percent-encoding
            "stream.testnet%2ebinance.vision",
            "stream.testnet%2Ebinance.vision",
            "stream.binance%2ecom",
            "stream.binance.com%00.evil.example",
            // backslash
            r"stream.testnet.binance.vision\evil.example",
            r"stream.binance.com\",
            // control characters and whitespace
            "stream.binance.co\tm",
            "stream.binance.co\nm",
            "stream.binance.co\rm",
            "stream.binance.com\u{0}",
            " stream.binance.com",
            "stream.binance.com ",
            "stream binance com",
            // non-ASCII
            "stre\u{0430}m.binance.com",
            // structurally ambiguous
            "",
            ":443",
            "a:b:c",
            "[::1",
            "[::1]x",
        ] {
            assert_eq!(
                host_of(authority),
                None,
                "parser must extract nothing from {authority:?}"
            );
        }
    }

    #[test]
    fn the_parser_extracts_exactly_the_host_from_a_well_formed_authority() {
        for (authority, expected) in [
            ("stream.binance.com", "stream.binance.com"),
            ("stream.binance.com:9443", "stream.binance.com"),
            ("stream.binance.com/ws", "stream.binance.com"),
            (
                "stream.binance.com:443/stream?streams=a/b",
                "stream.binance.com",
            ),
            ("stream.binance.com#frag", "stream.binance.com"),
            ("127.0.0.1:0", "127.0.0.1"),
            ("[::1]:9001", "[::1]"),
            ("[::1]", "[::1]"),
        ] {
            assert_eq!(host_of(authority), Some(expected), "{authority}");
        }
    }

    #[test]
    fn percent_encoding_in_the_authority_is_refused() {
        // A parser that percent-decodes would see a listed host here. We never
        // decode, and the allow-list means we never even compare.
        for url in [
            "wss://stream.testnet%2ebinance.vision/ws",
            "wss://stream.testnet%2Ebinance.vision/ws",
            "wss://stream%2Etestnet%2ebinance%2Evision/ws",
            "wss://stream.binance%2ecom/ws",
            "wss://stream.testnet.binance.vision%2f@evil.example/ws",
            "wss://stream.binance.com%00.evil.example/ws",
        ] {
            assert_eq!(classify(url), None, "{url} must not be recognised");
            assert!(!is_loopback(url), "{url}");
            assert!(require_class(EndpointClass::Testnet, url).is_err(), "{url}");
            assert!(
                require_class(EndpointClass::Production, url).is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn backslashes_are_refused() {
        // WHATWG URL parsing treats `\` as a path separator, so a backslash is a
        // way to make two parsers disagree about where the host ends.
        for url in [
            r"wss:\\stream.testnet.binance.vision\ws",
            r"wss://stream.testnet.binance.vision\@evil.example/ws",
            r"wss://evil.example\.stream.binance.com/ws",
            r"wss://stream.binance.com\/ws",
            r"wss://stream.binance.com\",
        ] {
            assert_eq!(classify(url), None, "{url} must not be recognised");
            assert!(require_class(EndpointClass::Testnet, url).is_err(), "{url}");
            assert!(
                require_class(EndpointClass::Production, url).is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn control_characters_and_whitespace_in_the_authority_are_refused() {
        // WHATWG strips tab/CR/LF *before* parsing, so `stream.binance.co\nm` is
        // one host to a browser and another to us. Either direction of
        // disagreement is a reason to refuse.
        for url in [
            "wss://stream.testnet.binance.vi\tsion/ws",
            "wss://stream.testnet.binance.vi\nsion/ws",
            "wss://stream.binance.co\rm/ws",
            "wss://stream.binance.com\t/ws",
            "wss://\tstream.binance.com/ws",
            "wss:// stream.binance.com/ws",
            "wss://stream.binance.com /ws",
            "wss://stream.testnet.binance.vision\u{0}/ws",
            "wss://stream binance com/ws",
        ] {
            assert_eq!(classify(url), None, "{url:?} must not be recognised");
            assert!(!is_loopback(url), "{url:?}");
            assert!(
                require_class(EndpointClass::Testnet, url).is_err(),
                "{url:?}"
            );
            assert!(
                require_class(EndpointClass::Production, url).is_err(),
                "{url:?}"
            );
        }
        // Leading/trailing whitespace around the whole URL fails at the scheme,
        // which is also a refusal - but assert it rather than assume it.
        for url in [
            " wss://stream.testnet.binance.vision/ws",
            "\twss://stream.binance.com/ws",
            "\nwss://stream.binance.com/ws",
        ] {
            assert_eq!(classify(url), None, "{url:?} must not be recognised");
        }
    }

    #[test]
    fn non_ascii_lookalike_hosts_are_refused() {
        // Cyrillic \u{0430} for ASCII `a`, and the punycode it encodes to.
        for url in [
            "wss://stre\u{0430}m.binance.com/ws",
            "wss://xn--strem-hnd.binance.com/ws",
            "wss://stream.testnet.binance.visi\u{03bf}n/ws",
        ] {
            assert_eq!(classify(url), None, "{url} must not be recognised");
        }
    }

    #[test]
    fn class_display_and_helpers_are_stable() {
        assert_eq!(EndpointClass::Testnet.to_string(), "testnet");
        assert_eq!(EndpointClass::Production.to_string(), "production");
        assert!(EndpointClass::Production.is_production());
        assert!(!EndpointClass::Testnet.is_production());
        assert_eq!(EndpointClass::Testnet.hosts(), TESTNET_HOSTS);
        assert_eq!(EndpointClass::Production.hosts(), PRODUCTION_HOSTS);
    }

    // --- the REST generalisation (milestone 3) ---
    //
    // Every test above is about `wss://` and must keep passing untouched: the
    // generalisation adds a second scheme pair, it does not loosen the first.

    #[test]
    fn canonical_rest_constants_classify_as_themselves() {
        assert_eq!(
            classify_for(Protocol::Rest, TESTNET_SPOT_REST_URL),
            Some(EndpointClass::Testnet),
            "{TESTNET_SPOT_REST_URL}"
        );
        assert_eq!(
            classify_for(Protocol::Rest, PRODUCTION_SPOT_REST_URL),
            Some(EndpointClass::Production),
            "{PRODUCTION_SPOT_REST_URL}"
        );
    }

    #[test]
    fn https_against_a_listed_host_classifies_by_host_exactly_as_wss_does() {
        // The point of the generalisation: one host list, two scheme pairs. The
        // host decides the class; the protocol only decides which schemes are
        // spellable.
        for host in TESTNET_HOSTS {
            for url in [
                format!("https://{host}"),
                format!("https://{host}/api/v3/exchangeInfo"),
                format!("https://{host}:443/api/v3/exchangeInfo?symbol=BTCUSDT"),
            ] {
                assert_eq!(
                    classify_for(Protocol::Rest, &url),
                    Some(EndpointClass::Testnet),
                    "{url}"
                );
            }
        }
        for host in PRODUCTION_HOSTS {
            let url = format!("https://{host}/api/v3/exchangeInfo");
            assert_eq!(
                classify_for(Protocol::Rest, &url),
                Some(EndpointClass::Production),
                "{url}"
            );
        }
    }

    #[test]
    fn the_two_scheme_pairs_do_not_leak_into_each_other() {
        // A REST URL is not a WebSocket endpoint and vice versa. This is what
        // keeps the WS gate exactly as strict as it was in milestone 2: the
        // `https://` case it used to refuse, it still refuses.
        assert_eq!(classify(TESTNET_SPOT_REST_URL), None);
        assert_eq!(classify(PRODUCTION_SPOT_REST_URL), None);
        assert_eq!(classify_for(Protocol::Rest, TESTNET_SPOT_WS_URL), None);
        assert_eq!(classify_for(Protocol::Rest, PRODUCTION_SPOT_WS_URL), None);

        for expected in [EndpointClass::Testnet, EndpointClass::Production] {
            assert!(require_class(expected, PRODUCTION_SPOT_REST_URL).is_err());
            assert!(require_class_for(Protocol::Rest, expected, PRODUCTION_SPOT_WS_URL).is_err());
        }
    }

    #[test]
    fn plaintext_http_against_a_real_host_is_not_recognised() {
        // TLS is not optional against the exchange, on either protocol.
        for url in [
            "http://api.binance.com/api/v3/exchangeInfo",
            "http://testnet.binance.vision/api/v3/exchangeInfo",
            "http://data-api.binance.vision/api/v3/exchangeInfo",
        ] {
            assert_eq!(classify_for(Protocol::Rest, url), None, "{url}");
            assert!(
                require_class_for(Protocol::Rest, EndpointClass::Testnet, url).is_err(),
                "{url}"
            );
            assert!(
                require_class_for(Protocol::Rest, EndpointClass::Production, url).is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn a_testnet_rest_config_aimed_at_a_production_host_refuses() {
        // The REST twin of THE test. A testnet-labelled run must never issue a
        // request against the live exchange, not even an unauthenticated one:
        // the filters it would come back with are the *production* symbol's, and
        // quantizing testnet orders against them is silent wrongness.
        for url in [
            PRODUCTION_SPOT_REST_URL,
            "https://api.binance.com/api/v3/exchangeInfo",
            "https://api3.binance.com",
            "https://data-api.binance.vision",
        ] {
            let err = require_class_for(Protocol::Rest, EndpointClass::Testnet, url)
                .expect_err("a testnet config must never reach a production host");
            assert_eq!(
                err,
                EndpointError::Mismatch {
                    expected: EndpointClass::Testnet,
                    found: EndpointClass::Production,
                    url: url.to_owned(),
                },
                "{url}"
            );
        }
    }

    #[test]
    fn a_production_rest_config_aimed_at_a_testnet_host_refuses() {
        for url in [
            TESTNET_SPOT_REST_URL,
            "https://testnet.binance.vision/api/v3/exchangeInfo",
        ] {
            assert_eq!(
                require_class_for(Protocol::Rest, EndpointClass::Production, url),
                Err(EndpointError::Mismatch {
                    expected: EndpointClass::Production,
                    found: EndpointClass::Testnet,
                    url: url.to_owned(),
                }),
                "{url}"
            );
        }
    }

    #[test]
    fn http_loopback_is_allowed_only_in_the_testnet_direction() {
        // The carve-out that lets the REST tests drive the real client against a
        // local fake HTTP server. Same shape, same narrowness, same reason as the
        // WebSocket one - and production still refuses it.
        for host in ["127.0.0.1:8080", "localhost", "[::1]:9001", "127.0.0.1"] {
            for scheme in ["http", "https"] {
                let url = format!("{scheme}://{host}/api/v3/exchangeInfo");

                assert!(is_loopback_for(Protocol::Rest, &url), "{url}");
                assert_eq!(classify_for(Protocol::Rest, &url), None, "{url}");
                assert_eq!(
                    require_class_for(Protocol::Rest, EndpointClass::Testnet, &url),
                    Ok(()),
                    "testnet must still reach a local fake server: {url}"
                );
                assert_eq!(
                    require_class_for(Protocol::Rest, EndpointClass::Production, &url),
                    Err(EndpointError::Unrecognised {
                        expected: EndpointClass::Production,
                        protocol: Protocol::Rest,
                        url: url.clone(),
                    }),
                    "production must refuse a loopback REST endpoint: {url}"
                );
            }
        }
        // A WebSocket-scheme loopback URL is not a REST endpoint, and the other
        // way round: the carve-out does not become a scheme-free back door.
        assert!(!is_loopback_for(Protocol::Rest, "ws://127.0.0.1:8080/ws"));
        assert!(!is_loopback_for(
            Protocol::WebSocket,
            "http://127.0.0.1:8080/"
        ));
    }

    #[test]
    fn the_hostile_authority_cases_refuse_under_https_too() {
        // The parser is shared, so this should hold by construction - which is
        // exactly why it is worth asserting: it pins that REST really does route
        // through `host_of` rather than through a second, sloppier check.
        for url in [
            "https://testnet.binance.vision@evil.example/api/v3/exchangeInfo",
            "https://user:pass@api.binance.com/api/v3/exchangeInfo",
            "https://testnet%2ebinance.vision/api/v3/exchangeInfo",
            r"https://api.binance.com\@evil.example/api",
            "https://api.binance.co\tm/api",
            "https://api.binance.com.evil.example/api",
            "https://notapi.binance.com/api",
            "https://binance.com/api",
            "https://\u{0430}pi.binance.com/api",
        ] {
            assert_eq!(classify_for(Protocol::Rest, url), None, "{url:?}");
            assert!(
                require_class_for(Protocol::Rest, EndpointClass::Testnet, url).is_err(),
                "{url:?}"
            );
            assert!(
                require_class_for(Protocol::Rest, EndpointClass::Production, url).is_err(),
                "{url:?}"
            );
        }
    }

    #[test]
    fn the_unrecognised_message_names_the_protocol_and_the_scheme_it_wanted() {
        let err = require_class_for(
            Protocol::Rest,
            EndpointClass::Testnet,
            "https://api.example.com",
        )
        .expect_err("must refuse the unknown");
        let msg = err.to_string();
        assert!(msg.contains("REST"), "{msg}");
        assert!(msg.contains("https://"), "{msg}");
        assert!(msg.contains("testnet.binance.vision"), "{msg}");

        let msg = require_class(EndpointClass::Testnet, "wss://stream.example.com")
            .expect_err("must refuse the unknown")
            .to_string();
        assert!(msg.contains("WebSocket"), "{msg}");
        assert!(msg.contains("wss://"), "{msg}");
    }

    #[test]
    fn protocol_display_and_schemes_are_stable() {
        assert_eq!(Protocol::WebSocket.to_string(), "WebSocket");
        assert_eq!(Protocol::Rest.to_string(), "REST");
        assert_eq!(Protocol::WebSocket.secure_scheme(), "wss://");
        assert_eq!(Protocol::WebSocket.plaintext_scheme(), "ws://");
        assert_eq!(Protocol::Rest.secure_scheme(), "https://");
        assert_eq!(Protocol::Rest.plaintext_scheme(), "http://");
    }
}
