//! The Binance combined-stream wire protocol: the SUBSCRIBE request, and the
//! four shapes of frame that can come back.
//!
//! # Why combined (`/stream`) and not raw (`/ws`)
//!
//! Confirmed against the current spot docs: wrapping is a property of the
//! *path*, not of the subscribe request. `/ws` is a raw stream and its payloads
//! arrive unwrapped; `/stream` is combined and every payload arrives as
//! `{"stream":"<name>","data":<rawPayload>}`.
//!
//! That wrapper is not a convenience, it is a correctness requirement here. With
//! more than one stream on a raw connection, nothing in the frame says which
//! stream it came from - and `@bookTicker` payloads carry no `"e"` event type at
//! all, so the only way to recover the name would be to guess from the shape.
//! [`crate::normalize`] treats the stream name as ground truth and cross-checks
//! the payload's `"s"` against it, [`crate::gap`] keys on it, and a recording
//! stores it as the thing that makes the file replayable. Guessing it would put
//! an inference on the path between the exchange and the archive.
//!
//! So the source connects to `/stream` bare and subscribes, and the stream name
//! is read off the wire rather than inferred.
//!
//! # Parsing rules
//!
//! Payloads are handed on as [`RawValue`], borrowed from the frame text and
//! never round-tripped through `serde_json::Value` - a `Value` object is a
//! `BTreeMap`, so parsing and reserialising would silently sort the keys and the
//! recording would stop being what Binance sent.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// The `method` field of a subscribe request.
const SUBSCRIBE: &str = "SUBSCRIBE";

/// One decoded frame from the combined stream.
///
/// No `PartialEq`: `RawValue` has none, and a payload is compared by its bytes
/// (`payload.get()`) rather than structurally - which is the comparison that
/// actually matters for an archive that must be byte-faithful.
#[derive(Debug)]
pub enum Frame<'a> {
    /// A market message: the stream it belongs to, and its payload verbatim.
    Data {
        stream: &'a str,
        payload: &'a RawValue,
    },
    /// A successful response to a request we sent, echoing its id.
    Ack { id: i64 },
    /// The exchange refused a request.
    Rejected {
        id: Option<i64>,
        code: i64,
        msg: String,
    },
    /// A well-formed JSON object that is none of the above - for instance the
    /// documented `serverShutdown` notice. Reported so it can be logged, not
    /// treated as data and not treated as fatal: an unrecognised *notice* is not
    /// missing market data, and disconnecting over one would turn a courtesy
    /// into an outage.
    Other,
}

/// A frame we could not make sense of. The message is dropped, never guessed at.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum WireError {
    #[error("frame is not valid JSON: {reason}")]
    NotJson { reason: String },

    #[error(
        "frame names stream `{stream}` but carries no `data`; refusing to \
         invent a payload for it"
    )]
    DataWithoutPayload { stream: String },

    #[error(
        "frame carries a `data` payload but no `stream` name. On a combined \
         connection every payload is named; an unnamed one cannot be routed, \
         normalized, or recorded, so it is dropped"
    )]
    PayloadWithoutStream,
}

/// Build a SUBSCRIBE request for `streams`.
///
/// The id is echoed back in the acknowledgement, which is how we know the
/// subscription actually took rather than assuming it did.
///
/// # Errors
///
/// Never fails in practice - the value is plain scalars and strings - but
/// returns the serializer's error rather than panicking on it.
pub fn subscribe_request(id: i64, streams: &[String]) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    struct Request<'a> {
        method: &'static str,
        params: &'a [String],
        id: i64,
    }

    serde_json::to_string(&Request {
        method: SUBSCRIBE,
        params: streams,
        id,
    })
}

/// Decode one text frame.
///
/// # Errors
///
/// [`WireError`] for anything that is not a JSON object, or that is a
/// half-formed data frame. Every variant means "drop this frame and say why".
pub fn parse_frame(text: &str) -> Result<Frame<'_>, WireError> {
    /// Every field is optional because one frame shape carries data, another
    /// carries an acknowledgement, and a third carries an error. Unknown fields
    /// are tolerated on purpose: Binance adds them, and refusing a frame over an
    /// additive change would take the feed down for a non-event.
    #[derive(Deserialize)]
    struct Raw<'a> {
        #[serde(borrow, default)]
        stream: Option<&'a str>,
        #[serde(borrow, default)]
        data: Option<&'a RawValue>,
        #[serde(default)]
        id: Option<i64>,
        #[serde(default)]
        error: Option<Rejection>,
        #[serde(borrow, default)]
        result: Option<&'a RawValue>,
    }

    #[derive(Deserialize)]
    struct Rejection {
        code: i64,
        msg: String,
    }

    let raw: Raw<'_> = serde_json::from_str(text).map_err(|source| WireError::NotJson {
        reason: source.to_string(),
    })?;

    if let Some(error) = raw.error {
        return Ok(Frame::Rejected {
            id: raw.id,
            code: error.code,
            msg: error.msg,
        });
    }

    match (raw.stream, raw.data) {
        (Some(stream), Some(payload)) => return Ok(Frame::Data { stream, payload }),
        (Some(stream), None) => {
            return Err(WireError::DataWithoutPayload {
                stream: stream.to_owned(),
            })
        }
        (None, Some(_)) => return Err(WireError::PayloadWithoutStream),
        (None, None) => {}
    }

    // A response to one of our requests. `result` is `null` for SUBSCRIBE; any
    // other result belongs to a method we do not send, so it is a notice rather
    // than our acknowledgement.
    let result_is_null = raw.result.is_none_or(|r| r.get().trim() == "null");
    match raw.id {
        Some(id) if result_is_null => Ok(Frame::Ack { id }),
        _ => Ok(Frame::Other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_subscribe_request_is_the_documented_shape() {
        let request = subscribe_request(
            1,
            &["btcusdt@bookTicker".to_owned(), "btcusdt@trade".to_owned()],
        )
        .expect("serialisable");
        assert_eq!(
            request,
            r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker","btcusdt@trade"],"id":1}"#
        );
    }

    #[test]
    fn a_data_frame_yields_its_stream_name_and_a_verbatim_payload() {
        let text = r#"{"stream":"btcusdt@trade","data":{"e":"trade","p":"0.001","t":1}}"#;
        let Frame::Data { stream, payload } = parse_frame(text).expect("a data frame") else {
            panic!("expected a data frame");
        };
        assert_eq!(stream, "btcusdt@trade");
        // Byte-for-byte, in the order Binance sent it. This is what gets
        // recorded, so any reordering here would corrupt the archive.
        assert_eq!(payload.get(), r#"{"e":"trade","p":"0.001","t":1}"#);
    }

    #[test]
    fn a_payload_is_not_reordered_or_renumbered_on_the_way_through() {
        let text = r#"{"stream":"btcusdt@bookTicker","data":{"u":4,"s":"BTCUSDT","b":"1.00000000","B":"0.10000000","a":"2.00000000","A":"0.20000000"}}"#;
        let Frame::Data { payload, .. } = parse_frame(text).expect("a data frame") else {
            panic!("expected a data frame");
        };
        assert_eq!(
            payload.get(),
            r#"{"u":4,"s":"BTCUSDT","b":"1.00000000","B":"0.10000000","a":"2.00000000","A":"0.20000000"}"#
        );
    }

    #[test]
    fn a_subscribe_acknowledgement_echoes_our_id() {
        let Frame::Ack { id } = parse_frame(r#"{"result":null,"id":7}"#).expect("an ack") else {
            panic!("expected an ack");
        };
        assert_eq!(id, 7);
    }

    #[test]
    fn a_rejection_carries_the_exchanges_own_code_and_message() {
        let text = r#"{"id":1,"error":{"code":2,"msg":"Invalid request: unknown variant"}}"#;
        let Frame::Rejected { id, code, msg } = parse_frame(text).expect("a rejection") else {
            panic!("expected a rejection");
        };
        assert_eq!((id, code), (Some(1), 2));
        assert_eq!(msg, "Invalid request: unknown variant");
    }

    #[test]
    fn an_unrecognised_notice_is_reported_rather_than_dropped_or_fatal() {
        // `serverShutdown` is documented but its shape is not, and a notice is
        // not missing market data. It must not take the connection down.
        for text in [
            r#"{"event":"serverShutdown"}"#,
            r#"{"result":["btcusdt@trade"],"id":9}"#,
        ] {
            assert!(
                matches!(parse_frame(text), Ok(Frame::Other)),
                "{text} should parse as a notice"
            );
        }
    }

    #[test]
    fn a_half_formed_data_frame_is_dropped_with_a_reason() {
        assert_eq!(
            parse_frame(r#"{"stream":"btcusdt@trade"}"#).expect_err("half-formed"),
            WireError::DataWithoutPayload {
                stream: "btcusdt@trade".to_owned()
            }
        );
        assert_eq!(
            parse_frame(r#"{"data":{"e":"trade"}}"#).expect_err("half-formed"),
            WireError::PayloadWithoutStream
        );
    }

    #[test]
    fn a_non_object_frame_is_refused() {
        for text in ["not json", "[1,2,3]", "null", ""] {
            assert!(
                matches!(parse_frame(text), Err(WireError::NotJson { .. })),
                "{text:?} should be refused"
            );
        }
    }
}
