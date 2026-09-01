//! The round-trip proof: a scripted session, recorded and read back, must
//! reproduce the **same event sequence** and the **same gap markers**.
//!
//! This file is the reason the recording format looks the way it does. Every
//! future backtest replays a file written by this code, so "the archive is
//! replay-shaped" cannot be an intention - it has to be an assertion.
//!
//! Nothing here touches the network. The "live" pass is a scripted list of
//! frames driven through exactly the code the connection layer will drive:
//! `normalize` for events, `SeqTracker` for gaps, `Recorder` for the file.

use std::path::Path;

use domain::MarketEvent;
use exchange::{
    normalize, read_session, DisconnectDetail, EndpointClass, GapDetail, GapKind, Marker,
    ReconnectDetail, Record, Recorder, RecorderConfig, Seq, SeqPolicy, SeqTracker,
};
use serde_json::value::RawValue;

const BOOK: &str = "btcusdt@bookTicker";
const TRADE: &str = "btcusdt@trade";
const START_NS: i64 = 1_712_340_878_000_000_000;

/// One scripted inbound event, in the shape the connection layer sees them.
enum Step {
    /// A raw frame, exactly as it arrives on the wire.
    Frame {
        stream: &'static str,
        json: String,
    },
    Disconnect {
        reason: &'static str,
    },
    Reconnect {
        attempt: u32,
        waited_ms: u64,
    },
}

/// Note the key order: `u` before `s` before `b`. It is not alphabetical, which
/// is what a parse-and-reserialize round trip through `serde_json::Value` would
/// silently impose - `Map` is a `BTreeMap`. Recording the raw bytes is what
/// keeps the archive byte-identical to what Binance sent.
fn book_frame(update_id: i64) -> String {
    format!(
        r#"{{"u":{update_id},"s":"BTCUSDT","b":"64999.99000000","B":"0.03984000","a":"65000.00000000","A":"1.42150000"}}"#
    )
}

fn trade_frame(trade_id: i64, event_ms: i64) -> String {
    format!(
        r#"{{"e":"trade","E":{event_ms},"s":"BTCUSDT","t":{trade_id},"p":"65000.01000000","q":"0.00099000","T":{event_ms},"m":true,"M":true}}"#
    )
}

/// The scripted session. Deliberately exercises all four gap shapes: a counted
/// miss, a regression, and an outage under each policy.
fn script() -> Vec<Step> {
    vec![
        Step::Frame {
            stream: BOOK,
            json: book_frame(1_000),
        },
        Step::Frame {
            stream: TRADE,
            json: trade_frame(500, 1_712_340_878_001),
        },
        // A forward leap in an order book updateId: normal, must not be a gap.
        Step::Frame {
            stream: BOOK,
            json: book_frame(1_005),
        },
        Step::Frame {
            stream: TRADE,
            json: trade_frame(501, 1_712_340_878_002),
        },
        // A counted miss: trade ids are contiguous, so 38 trades were lost.
        Step::Frame {
            stream: TRADE,
            json: trade_frame(540, 1_712_340_878_100),
        },
        // A regression on the book: detectable under a monotonic policy.
        Step::Frame {
            stream: BOOK,
            json: book_frame(1_004),
        },
        Step::Disconnect {
            reason: "connection reset by peer",
        },
        Step::Reconnect {
            attempt: 2,
            waited_ms: 1_500,
        },
        // First message per stream after the outage.
        Step::Frame {
            stream: BOOK,
            json: book_frame(2_000),
        },
        Step::Frame {
            stream: TRADE,
            json: trade_frame(545, 1_712_340_879_000),
        },
        Step::Frame {
            stream: TRADE,
            json: trade_frame(546, 1_712_340_879_001),
        },
    ]
}

/// Our ingest clock, advanced one millisecond per step. Scripted rather than
/// read, so the whole test is deterministic and offline.
fn recv_ns(step: usize) -> i64 {
    START_NS + (step as i64) * 1_000_000
}

fn config(dir: &Path) -> RecorderConfig {
    RecorderConfig {
        dir: dir.to_path_buf(),
        started_ns: START_NS,
        endpoint: EndpointClass::Testnet,
        ws_url: "wss://stream.testnet.binance.vision/ws".to_owned(),
        symbols: vec!["BTCUSDT".to_owned()],
        streams: vec![BOOK.to_owned(), TRADE.to_owned()],
    }
}

/// What one pass over the session observed.
#[derive(Debug, Default, PartialEq, Eq)]
struct Observed {
    events: Vec<MarketEvent>,
    gaps: Vec<GapDetail>,
}

/// The live pass: normalize, track gaps, record. This is the code path the
/// connection layer will drive frame-for-frame.
fn record_live(dir: &Path) -> (Observed, std::path::PathBuf) {
    let mut recorder = Recorder::create(&config(dir)).expect("recording target should be usable");
    let mut tracker = SeqTracker::new();
    let mut observed = Observed::default();

    for (index, step) in script().iter().enumerate() {
        let now = recv_ns(index);
        match step {
            Step::Frame { stream, json } => {
                let raw = RawValue::from_string(json.clone()).expect("frame is valid JSON");

                // Record the raw bytes first: what we received is a fact
                // independent of whether we can make sense of it.
                recorder
                    .record_payload(now, stream, &raw)
                    .expect("recording should not fail");

                let payload = serde_json::from_str(raw.get()).expect("valid JSON");
                let normalized = normalize(stream, &payload, now).expect("scripted frames parse");

                if let Some(gap) = tracker.observe(stream, normalized.seq) {
                    recorder
                        .record_marker(now, Marker::Gap(gap.clone()))
                        .expect("recording should not fail");
                    observed.gaps.push(gap);
                }
                observed.events.push(normalized.event);
            }
            Step::Disconnect { reason } => {
                recorder
                    .record_marker(
                        now,
                        Marker::Disconnect(DisconnectDetail {
                            reason: (*reason).to_owned(),
                        }),
                    )
                    .expect("recording should not fail");
                tracker.mark_outage();
            }
            Step::Reconnect { attempt, waited_ms } => {
                recorder
                    .record_marker(
                        now,
                        Marker::Reconnect(ReconnectDetail {
                            attempt: *attempt,
                            waited_ms: *waited_ms,
                        }),
                    )
                    .expect("recording should not fail");
            }
        }
    }

    let path = recorder.finish().expect("clean close");
    (observed, path)
}

/// The replay pass: read the file, run the *same* `normalize`, and re-derive
/// gaps with a fresh tracker driven by the recorded disconnect markers.
///
/// Re-deriving rather than only reading back the gap markers is the point. If
/// the file did not carry enough - the raw payloads, the ingest timestamps, and
/// the disconnects - the re-derived gaps would diverge from the recorded ones,
/// and this is where that would show.
fn replay(path: &Path) -> (Observed, Vec<GapDetail>) {
    let (header, records) = read_session(path).expect("recording should be readable");
    assert_eq!(header.endpoint, EndpointClass::Testnet);

    let mut tracker = SeqTracker::new();
    let mut rederived = Observed::default();
    let mut recorded_gaps = Vec::new();

    for record in records {
        match record {
            Record::Data(data) => {
                let normalized = data.normalized().expect("recorded payloads re-normalize");
                if let Some(gap) = tracker.observe(&data.stream, normalized.seq) {
                    rederived.gaps.push(gap);
                }
                rederived.events.push(normalized.event);
            }
            Record::Marker(marker) => match marker.marker {
                Marker::Gap(detail) => recorded_gaps.push(detail),
                Marker::Disconnect(_) => tracker.mark_outage(),
                Marker::Reconnect(_) => {}
            },
        }
    }

    (rederived, recorded_gaps)
}

#[test]
fn a_recorded_session_replays_to_the_same_events_and_the_same_gaps() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (live, path) = record_live(dir.path());
    let (rederived, recorded_gaps) = replay(&path);

    // 1. The same event sequence, exactly.
    assert_eq!(
        rederived.events, live.events,
        "replay must produce the identical event sequence"
    );
    assert_eq!(live.events.len(), 9, "nine frames in the script");

    // 2. The same gap markers, exactly - both as written to the file and as
    //    re-derived from the replayed data under the recorded semantics.
    assert_eq!(
        recorded_gaps, live.gaps,
        "the gap markers in the file must be the gaps that were seen live"
    );
    assert_eq!(
        rederived.gaps, live.gaps,
        "re-deriving gaps from the replayed records must reach the same verdicts"
    );
}

#[test]
fn the_recorded_gaps_are_the_four_expected_shapes_with_their_policies() {
    // Spells the expected gaps out rather than only comparing two runs of the
    // same code to each other, which would agree even if both were wrong.
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let (_, recorded_gaps) = replay(&path);

    assert_eq!(
        recorded_gaps,
        vec![
            // A contiguous stream can count what it lost.
            GapDetail {
                stream: TRADE.to_owned(),
                policy: SeqPolicy::Contiguous,
                kind: GapKind::Missed,
                from: 501,
                to: 540,
                missing: Some(38),
            },
            // A monotonic stream can only see the id going backwards.
            GapDetail {
                stream: BOOK.to_owned(),
                policy: SeqPolicy::Monotonic,
                kind: GapKind::Regressed,
                from: 1_005,
                to: 1_004,
                missing: None,
            },
            // Across the outage: unknowable under a monotonic policy...
            GapDetail {
                stream: BOOK.to_owned(),
                policy: SeqPolicy::Monotonic,
                kind: GapKind::Outage,
                from: 1_004,
                to: 2_000,
                missing: None,
            },
            // ...and countable under a contiguous one.
            GapDetail {
                stream: TRADE.to_owned(),
                policy: SeqPolicy::Contiguous,
                kind: GapKind::Outage,
                from: 540,
                to: 545,
                missing: Some(4),
            },
        ]
    );
}

#[test]
fn a_forward_leap_in_the_order_book_update_id_is_not_recorded_as_a_gap() {
    // The correction that motivated recording the policy. `u` jumping 1000 ->
    // 1005 is ordinary; flagging it would bury the gaps that matter.
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let (_, recorded_gaps) = replay(&path);

    assert!(
        !recorded_gaps
            .iter()
            .any(|g| g.stream == BOOK && g.kind == GapKind::Missed),
        "a monotonic stream must never report a counted miss: {recorded_gaps:?}"
    );
}

#[test]
fn replaying_under_the_wrong_policy_would_disagree() {
    // Shows the policy is load-bearing rather than decorative: read the same
    // recorded ids back as if the book ticker were contiguous, and the verdicts
    // change. This is why the marker carries the policy instead of a reader
    // assuming one.
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let (_, records) = read_session(&path).expect("readable");

    let mut honest = SeqTracker::new();
    let mut wrong = SeqTracker::new();
    let (mut honest_gaps, mut wrong_gaps) = (0_usize, 0_usize);

    for record in records {
        if let Record::Data(data) = record {
            let seq = data.normalized().expect("re-normalizes").seq;
            honest_gaps += usize::from(honest.observe(&data.stream, seq).is_some());
            // The same ids, forced to contiguous semantics.
            let forced = Seq {
                id: seq.id,
                policy: SeqPolicy::Contiguous,
            };
            wrong_gaps += usize::from(wrong.observe(&data.stream, forced).is_some());
        }
    }

    assert!(
        wrong_gaps > honest_gaps,
        "forcing contiguous semantics onto the book ticker should invent gaps \
         ({wrong_gaps} vs {honest_gaps}); if it does not, the policy is not doing anything"
    );
}

#[test]
fn payloads_are_stored_byte_for_byte_including_key_order() {
    // A parse-and-reserialize round trip through `serde_json::Value` would sort
    // these keys, because `Map` is a `BTreeMap`. The archive must be what
    // Binance sent, not a normalised rendering of it.
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let (_, records) = read_session(&path).expect("readable");

    let sent: Vec<String> = script()
        .into_iter()
        .filter_map(|s| match s {
            Step::Frame { json, .. } => Some(json),
            _ => None,
        })
        .collect();

    let stored: Vec<String> = records
        .iter()
        .filter_map(|r| match r {
            Record::Data(d) => Some(d.payload.get().to_owned()),
            Record::Marker(_) => None,
        })
        .collect();

    assert_eq!(
        stored, sent,
        "payloads must come back exactly as they went in"
    );
    assert!(
        stored[0].starts_with(r#"{"u":"#),
        "key order must survive the read: {}",
        stored[0]
    );

    // And the same check against the file on disk, so this holds even if the
    // reader and the writer ever agreed on the same wrong thing.
    let text = std::fs::read_to_string(&path).expect("readable");
    for frame in &sent {
        assert!(
            text.contains(frame.as_str()),
            "the raw frame is not in the file verbatim: {frame}"
        );
    }
}

#[test]
fn ingest_metadata_is_recorded_and_drives_the_replayed_timestamps() {
    // `@bookTicker` carries no exchange timestamp, so its event time is our
    // ingest clock. That only replays correctly because `recv_ns` is on every
    // line - this asserts the whole chain, not just that the field exists.
    let dir = tempfile::tempdir().expect("temp dir");
    let (live, path) = record_live(dir.path());
    let (_, records) = read_session(&path).expect("readable");

    let data: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            Record::Data(d) => Some(d),
            Record::Marker(_) => None,
        })
        .collect();

    // Step 0 was a book ticker; its event time is that step's ingest clock.
    assert_eq!(data[0].recv_ns, recv_ns(0));
    let MarketEvent::BookTicker(first) = &live.events[0] else {
        panic!("expected a book ticker first");
    };
    assert_eq!(first.event_time, recv_ns(0) / 1_000_000);

    // And every recorded ingest time is the one the script handed out.
    for (record, expected) in data.iter().zip([0, 1, 2, 3, 4, 5, 8, 9, 10]) {
        assert_eq!(record.recv_ns, recv_ns(expected));
    }
}

#[test]
fn the_ingest_counter_is_monotonic_across_data_and_markers_alike() {
    // `seq` counts what we received, in order, across both kinds of line - the
    // property that lets a replay reconstruct the true interleaving of data and
    // health events.
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let (_, records) = read_session(&path).expect("readable");

    let seqs: Vec<u64> = records.iter().map(Record::seq).collect();
    assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());
    assert!(
        records.iter().any(|r| matches!(r, Record::Marker(_))),
        "the script should have produced markers"
    );
}

#[test]
fn the_header_describes_the_session_it_recorded() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let (header, _) = read_session(&path).expect("readable");

    assert_eq!(header.format, exchange::FORMAT);
    assert_eq!(header.version, exchange::FORMAT_VERSION);
    assert_eq!(header.started_ns, START_NS);
    assert_eq!(header.endpoint, EndpointClass::Testnet);
    assert_eq!(header.symbols, vec!["BTCUSDT".to_owned()]);
    assert_eq!(header.streams, vec![BOOK.to_owned(), TRADE.to_owned()]);

    // The filename names the session too, so a directory listing is legible.
    let name = path.file_name().and_then(|n| n.to_str()).expect("filename");
    assert_eq!(name, "binance-20240405T181438Z-BTCUSDT.ndjson");
}

#[test]
fn the_file_is_newline_delimited_json_one_record_per_line() {
    let dir = tempfile::tempdir().expect("temp dir");
    let (_, path) = record_live(dir.path());
    let text = std::fs::read_to_string(&path).expect("readable");

    assert!(
        text.ends_with('\n'),
        "every line, including the last, terminates"
    );
    for line in text.lines() {
        let value: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("line is not JSON: {line}: {e}"));
        assert!(value.is_object(), "every line is a JSON object: {line}");
        assert!(!line.contains('\n'), "records never span lines");
    }
    // Header, plus one line per script step that produced something.
    assert_eq!(text.lines().count(), 1 + 9 + 4 + 2);
}
