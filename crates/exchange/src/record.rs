//! Append-only NDJSON recording of the raw inbound stream.
//!
//! # What is recorded, and why it is the raw payload
//!
//! Raw inbound payloads plus ingest metadata - **not** normalized events.
//! Normalization is a pure function applied identically on live and on replay,
//! so recording raw means a later fix to [`crate::normalize`] re-derives correct
//! events from history. Recording normalized events would instead leave a
//! permanently corrupted archive that no fix can reach.
//!
//! Payloads are stored byte-for-byte. They travel as [`RawValue`] from the
//! socket to the file without a parse-and-reserialize round trip, so key order,
//! spacing, and numeric spelling survive exactly as Binance sent them.
//!
//! # Layout
//!
//! One session per file, newline-delimited JSON, append-only:
//!
//! ```text
//! {"format":"binance-market-ndjson","version":1,"started_ns":...,...}
//! {"recv_ns":...,"seq":0,"stream":"btcusdt@bookTicker","payload":{...}}
//! {"recv_ns":...,"seq":1,"marker":"gap","detail":{...}}
//! ```
//!
//! Health markers are written **inline**, in stream order, so a replay
//! reproduces the same gap flags at the same points rather than recomputing them
//! and hoping to agree. Every gap marker carries the [`SeqPolicy`] that applied
//! (see [`crate::gap`]) - a contiguous-stream marker and a monotonic-stream
//! marker are not interchangeable, and a reader must not have to guess which it
//! is holding.
//!
//! # Fail-closed
//!
//! [`Recorder::create`] proves the target is writable by writing and flushing
//! the header before it returns. If recording is enabled and that fails, the
//! source refuses to start. Running unrecorded while believing otherwise is the
//! one failure that quietly destroys the archive: it produces no error, no gap,
//! and no way to tell afterwards which sessions were real.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::endpoint::EndpointClass;
use crate::gap::GapDetail;
use crate::normalize::{normalize, NormalizeError, Normalized};

/// Identifies the file layout. A reader that does not recognise this refuses the
/// file rather than interpreting it hopefully.
pub const FORMAT: &str = "binance-market-ndjson";

/// The layout version this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;

/// File extension for a session recording.
pub const EXTENSION: &str = "ndjson";

/// Bound on the disambiguating suffix when a session filename is already taken.
const MAX_FILENAME_ATTEMPTS: u32 = 100;

// --- file shape ---

/// The first line of every recording: what this file is, and what produced it.
///
/// Not in the original milestone sketch, and worth the deviation. A recording is
/// read back months later by a backtester that must be able to tell a testnet
/// capture from a production one, and must refuse a format it does not
/// understand rather than misparse it. Both facts have to live in the file,
/// because a filename does not survive being moved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub format: String,
    pub version: u32,
    /// Session start on our ingest clock, nanoseconds since the Unix epoch.
    pub started_ns: i64,
    /// Which exchange environment this capture came from. Never inferred from
    /// the URL at read time - recorded, so it cannot drift.
    pub endpoint: EndpointClass,
    pub ws_url: String,
    pub symbols: Vec<String>,
    pub streams: Vec<String>,
}

/// One recorded inbound message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataRecord {
    /// Our ingest clock, nanoseconds since the Unix epoch.
    pub recv_ns: i64,
    /// Our own monotonic counter, assigned on ingest. Distinct from any
    /// exchange-side id: this counts what we received, in the order we received
    /// it, across every stream and marker in the session.
    pub seq: u64,
    pub stream: String,
    /// The Binance payload, verbatim.
    pub payload: Box<RawValue>,
}

/// One recorded health marker, written inline between data records.
///
/// Serialised by hand rather than by `#[serde(flatten)]` or an adjacently-tagged
/// enum. Both of those expand to a content buffer that implements `visit_f64`,
/// which the workspace's no-float lint refuses - correctly, since a buffered
/// float is exactly the path a number could take into this crate without anyone
/// choosing it. The explicit form below has no float visitor anywhere in it and
/// produces the line shape the format specifies, in that field order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerRecord {
    pub recv_ns: i64,
    pub seq: u64,
    pub marker: Marker,
}

/// What a health marker says. Written as `"marker": <kind>, "detail": {...}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Marker {
    /// Messages are missing, or the sequence went backwards. Carries the policy
    /// under which that judgement was made.
    Gap(GapDetail),
    Disconnect(DisconnectDetail),
    Reconnect(ReconnectDetail),
}

/// The `marker` discriminant, as it appears on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarkerKind {
    Gap,
    Disconnect,
    Reconnect,
}

impl Marker {
    #[must_use]
    pub fn kind(&self) -> MarkerKind {
        match self {
            Self::Gap(_) => MarkerKind::Gap,
            Self::Disconnect(_) => MarkerKind::Disconnect,
            Self::Reconnect(_) => MarkerKind::Reconnect,
        }
    }
}

/// The wire form, written out. Field order here is the file's field order.
#[derive(Serialize)]
struct MarkerLineOut<'a, D: Serialize> {
    recv_ns: i64,
    seq: u64,
    marker: MarkerKind,
    detail: &'a D,
}

/// The wire form, read back. `detail` is parsed in a second step, once the kind
/// says which shape to expect, so a wrong-shaped detail names its own marker
/// kind in the error instead of failing as "no variant matched".
#[derive(Deserialize)]
struct MarkerLineIn {
    recv_ns: i64,
    seq: u64,
    marker: MarkerKind,
    detail: serde_json::Value,
}

impl Serialize for MarkerRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (recv_ns, seq, marker) = (self.recv_ns, self.seq, self.marker.kind());
        match &self.marker {
            Marker::Gap(detail) => MarkerLineOut {
                recv_ns,
                seq,
                marker,
                detail,
            }
            .serialize(serializer),
            Marker::Disconnect(detail) => MarkerLineOut {
                recv_ns,
                seq,
                marker,
                detail,
            }
            .serialize(serializer),
            Marker::Reconnect(detail) => MarkerLineOut {
                recv_ns,
                seq,
                marker,
                detail,
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MarkerRecord {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;

        let line = MarkerLineIn::deserialize(deserializer)?;
        let detail = line.detail;
        let marker = match line.marker {
            MarkerKind::Gap => serde_json::from_value(detail).map(Marker::Gap),
            MarkerKind::Disconnect => serde_json::from_value(detail).map(Marker::Disconnect),
            MarkerKind::Reconnect => serde_json::from_value(detail).map(Marker::Reconnect),
        }
        .map_err(|e| {
            D::Error::custom(format!(
                "`{:?}` marker has a malformed detail: {e}",
                line.marker
            ))
        })?;

        Ok(Self {
            recv_ns: line.recv_ns,
            seq: line.seq,
            marker,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisconnectDetail {
    /// Why the connection ended. Operator-facing text, never a payload or a
    /// credential.
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconnectDetail {
    /// Which attempt succeeded, counting from one after the drop.
    pub attempt: u32,
    /// How long we waited before this attempt, including jitter.
    pub waited_ms: u64,
}

/// One line of a recording, after the header.
#[derive(Clone, Debug)]
pub enum Record {
    Data(DataRecord),
    Marker(MarkerRecord),
}

impl Record {
    /// The ingest sequence number, whichever kind of line this is.
    #[must_use]
    pub fn seq(&self) -> u64 {
        match self {
            Self::Data(d) => d.seq,
            Self::Marker(m) => m.seq,
        }
    }

    /// The ingest timestamp, whichever kind of line this is.
    #[must_use]
    pub fn recv_ns(&self) -> i64 {
        match self {
            Self::Data(d) => d.recv_ns,
            Self::Marker(m) => m.recv_ns,
        }
    }
}

impl DataRecord {
    /// Run the recorded payload back through the live normalization path.
    ///
    /// This is replay, in one line: the same [`normalize`] function, the same
    /// stream name, and the same ingest timestamp that were used live. Nothing
    /// here re-derives or approximates anything.
    ///
    /// # Errors
    ///
    /// [`NormalizeError`], exactly as the live path would have produced it.
    pub fn normalized(&self) -> Result<Normalized, NormalizeError> {
        let payload =
            serde_json::from_str(self.payload.get()).map_err(|_| NormalizeError::NotAnObject {
                stream: self.stream.clone(),
                found: "unparseable JSON",
            })?;
        normalize(&self.stream, &payload, self.recv_ns)
    }
}

// --- errors ---

/// Everything that can go wrong writing or reading a recording. Every write-side
/// variant is a refusal to run: there is no degraded mode where the bot keeps
/// trading while the archive silently stops.
#[derive(thiserror::Error, Debug)]
pub enum RecordError {
    #[error(
        "recording is enabled but the directory {} could not be prepared; \
         refusing to start rather than run unrecorded",
        .dir.display()
    )]
    DirUnusable {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "recording is enabled but the session file {} could not be created; \
         refusing to start rather than run unrecorded",
        .path.display()
    )]
    FileUnopenable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "recording is enabled but {} could not be written to; \
         refusing to start rather than run unrecorded",
        .path.display()
    )]
    NotWritable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("writing to the recording {} failed", .path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("reading the recording {} failed", .path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{}: line {line} is not a valid record: {reason}", .path.display())]
    MalformedRecord {
        path: PathBuf,
        line: usize,
        reason: String,
    },

    #[error(
        "{} is not a `{FORMAT}` recording: line 1 must be the format header",
        .path.display()
    )]
    MissingHeader { path: PathBuf },

    #[error(
        "{} is recording format `{found}` version {version}; \
         this build reads `{FORMAT}` version {FORMAT_VERSION}",
        .path.display()
    )]
    UnsupportedFormat {
        path: PathBuf,
        found: String,
        version: u32,
    },

    #[error("session start time {started_ns}ns is negative; refusing to name a session with it")]
    InvalidStartTime { started_ns: i64 },

    #[error(
        "could not find an unused session filename in {} after {MAX_FILENAME_ATTEMPTS} \
         attempts; refusing rather than appending to another session's file",
        .dir.display()
    )]
    FilenameExhausted { dir: PathBuf },
}

// --- writing ---

/// What a session recording is opened with.
#[derive(Clone, Debug)]
pub struct RecorderConfig {
    pub dir: PathBuf,
    /// Session start on our ingest clock. Also names the file.
    pub started_ns: i64,
    pub endpoint: EndpointClass,
    pub ws_url: String,
    pub symbols: Vec<String>,
    pub streams: Vec<String>,
}

/// An append-only NDJSON session writer.
///
/// Buffered, and flushed on every marker: markers are rare and are exactly the
/// health facts worth having on disk immediately. A hard kill can therefore lose
/// the tail of the data buffer, which is an accepted trade for not making a
/// syscall per book update. Call [`Recorder::finish`] on a clean shutdown to
/// flush the rest and learn whether it worked.
#[derive(Debug)]
pub struct Recorder {
    path: PathBuf,
    writer: BufWriter<File>,
    next_seq: u64,
}

impl Recorder {
    /// Open a session recording, proving as it goes that the target is usable.
    ///
    /// The directory is created if absent, the file is opened without ever
    /// clobbering an existing one, and the header is written **and flushed**
    /// before this returns. That last step is the point: a permissions or
    /// disk-space problem surfaces here, at startup, rather than at the first
    /// message - by which time the operator believes the session is recorded.
    ///
    /// # Errors
    ///
    /// [`RecordError::DirUnusable`], [`RecordError::FileUnopenable`], or
    /// [`RecordError::NotWritable`] - each of which means the caller must refuse
    /// to start, not continue unrecorded.
    pub fn create(config: &RecorderConfig) -> Result<Self, RecordError> {
        if config.started_ns < 0 {
            return Err(RecordError::InvalidStartTime {
                started_ns: config.started_ns,
            });
        }

        std::fs::create_dir_all(&config.dir).map_err(|source| RecordError::DirUnusable {
            dir: config.dir.clone(),
            source,
        })?;

        let (path, file) = open_new_session_file(config)?;

        let mut recorder = Self {
            path,
            writer: BufWriter::new(file),
            next_seq: 0,
        };

        let header = Header {
            format: FORMAT.to_owned(),
            version: FORMAT_VERSION,
            started_ns: config.started_ns,
            endpoint: config.endpoint,
            ws_url: config.ws_url.clone(),
            symbols: config.symbols.clone(),
            streams: config.streams.clone(),
        };

        // Write *and flush* the header now. An unwritable target must fail here,
        // where it can still stop the process, rather than at the first payload.
        recorder
            .write_line(&header)
            .and_then(|()| recorder.flush())
            .map_err(|error| match error {
                RecordError::Write { path, source } => RecordError::NotWritable { path, source },
                other => other,
            })?;

        Ok(recorder)
    }

    /// Where this session is being written.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The sequence number the next record will be given.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Append one raw inbound payload. Returns the sequence number assigned.
    ///
    /// `payload` is written through unchanged; it is never parsed here.
    ///
    /// # Errors
    ///
    /// [`RecordError::Write`]. A failed write is fatal to the session: the
    /// caller must stop rather than continue with a recording that has a hole in
    /// it and no record of where.
    pub fn record_payload(
        &mut self,
        recv_ns: i64,
        stream: &str,
        payload: &RawValue,
    ) -> Result<u64, RecordError> {
        #[derive(Serialize)]
        struct DataLine<'a> {
            recv_ns: i64,
            seq: u64,
            stream: &'a str,
            payload: &'a RawValue,
        }

        let seq = self.take_seq();
        self.write_line(&DataLine {
            recv_ns,
            seq,
            stream,
            payload,
        })?;
        Ok(seq)
    }

    /// Append one health marker, and flush.
    ///
    /// Markers are flushed immediately because they are rare and because they
    /// are the facts an operator goes looking for after something went wrong -
    /// the worst possible thing to lose in a buffer.
    ///
    /// # Errors
    ///
    /// [`RecordError::Write`], which is fatal to the session as above.
    pub fn record_marker(&mut self, recv_ns: i64, marker: Marker) -> Result<u64, RecordError> {
        let seq = self.take_seq();
        self.write_line(&MarkerRecord {
            recv_ns,
            seq,
            marker,
        })?;
        self.flush()?;
        Ok(seq)
    }

    /// Push buffered records to the file.
    ///
    /// # Errors
    ///
    /// [`RecordError::Write`].
    pub fn flush(&mut self) -> Result<(), RecordError> {
        self.writer.flush().map_err(|source| RecordError::Write {
            path: self.path.clone(),
            source,
        })
    }

    /// Flush and close, returning the path written.
    ///
    /// Consuming, and fallible on purpose: `Drop` cannot report an error, so a
    /// clean shutdown says so here instead of discovering afterwards that the
    /// last few seconds never reached the disk.
    ///
    /// # Errors
    ///
    /// [`RecordError::Write`].
    pub fn finish(mut self) -> Result<PathBuf, RecordError> {
        self.flush()?;
        Ok(self.path)
    }

    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn write_line<T: Serialize>(&mut self, value: &T) -> Result<(), RecordError> {
        let map_err = |source| RecordError::Write {
            path: self.path.clone(),
            source,
        };
        // `to_writer` on a struct of plain scalars and pre-validated JSON cannot
        // fail for any reason except the underlying write, so an error here is
        // an I/O error either way.
        serde_json::to_writer(&mut self.writer, value)
            .map_err(std::io::Error::from)
            .map_err(map_err)?;
        self.writer.write_all(b"\n").map_err(map_err)
    }
}

/// Open a session file without ever clobbering or appending to an existing one.
///
/// `create_new` means two runs can never end up interleaved in one file. On a
/// name collision - a restart inside the same second with the same symbols - a
/// bounded numeric suffix is tried rather than either overwriting history or
/// refusing to run over a cosmetic clash.
fn open_new_session_file(config: &RecorderConfig) -> Result<(PathBuf, File), RecordError> {
    let base = session_filename(config.started_ns, &config.symbols)?;

    for attempt in 0..MAX_FILENAME_ATTEMPTS {
        let name = if attempt == 0 {
            format!("{base}.{EXTENSION}")
        } else {
            format!("{base}-{attempt}.{EXTENSION}")
        };
        let path = config.dir.join(name);

        match OpenOptions::new().create_new(true).append(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(RecordError::FileUnopenable { path, source }),
        }
    }

    Err(RecordError::FilenameExhausted {
        dir: config.dir.clone(),
    })
}

/// Build the session filename stem: `binance-<UTC timestamp>-<symbols>`.
///
/// # Errors
///
/// [`RecordError::InvalidStartTime`] for a pre-epoch start time.
pub fn session_filename(started_ns: i64, symbols: &[String]) -> Result<String, RecordError> {
    if started_ns < 0 {
        return Err(RecordError::InvalidStartTime { started_ns });
    }

    // Long symbol sets make unusable filenames, so they are summarised. The full
    // set is always in the header, which is where a reader should look anyway.
    let joined = symbols.join("_");
    let symbol_part = if symbols.is_empty() {
        "nosymbols".to_owned()
    } else if joined.len() <= 48 {
        joined
    } else {
        format!("{}_and{}more", symbols[0], symbols.len() - 1)
    };

    Ok(format!("binance-{}-{symbol_part}", utc_stamp(started_ns)))
}

/// Format a Unix nanosecond timestamp as `YYYYMMDDTHHMMSSZ`.
///
/// Hand-rolled rather than adding a date library for one format string. Integer
/// arithmetic only - no float can appear anywhere near this crate - and pinned by
/// tests against known dates, leap years, and the epoch itself.
fn utc_stamp(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let second_of_day = secs.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60,
        second_of_day % 60,
    );

    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Days since 1970-01-01 to a proleptic Gregorian calendar date.
///
/// Howard Hinnant's `civil_from_days`, which is exact in integer arithmetic for
/// the whole representable range.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };

    (year + i64::from(month <= 2), month, day)
}

// --- reading ---

/// Streaming reader over a session recording.
///
/// An iterator rather than a `Vec`, so a multi-gigabyte capture replays without
/// being held in memory. Each item is one line; a malformed line is a typed
/// error rather than a silent skip, because a replay that quietly drops records
/// is a backtest that quietly lies.
#[derive(Debug)]
pub struct RecordReader<R> {
    path: PathBuf,
    lines: std::io::Lines<R>,
    line_number: usize,
}

/// Open a recording, returning its header and a reader over the records.
///
/// # Errors
///
/// [`RecordError::Read`] if the file cannot be opened or read,
/// [`RecordError::MissingHeader`] if line 1 is not a format header, and
/// [`RecordError::UnsupportedFormat`] if it names a format or version this build
/// does not read. A file we cannot vouch for is refused, never partially
/// interpreted.
pub fn open_session(
    path: impl AsRef<Path>,
) -> Result<(Header, RecordReader<BufReader<File>>), RecordError> {
    let path = path.as_ref().to_path_buf();
    let file = File::open(&path).map_err(|source| RecordError::Read {
        path: path.clone(),
        source,
    })?;
    read_header(path, BufReader::new(file))
}

/// Read a whole recording into memory: the header, then every record.
///
/// Convenient for tests and small captures. Prefer [`open_session`] for anything
/// that might be large.
///
/// # Errors
///
/// As [`open_session`], plus [`RecordError::MalformedRecord`] for any line that
/// is not a valid record.
pub fn read_session(path: impl AsRef<Path>) -> Result<(Header, Vec<Record>), RecordError> {
    let (header, reader) = open_session(path)?;
    let records = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((header, records))
}

fn read_header<R: BufRead>(
    path: PathBuf,
    reader: R,
) -> Result<(Header, RecordReader<R>), RecordError> {
    let mut lines = reader.lines();

    let first = lines
        .next()
        .transpose()
        .map_err(|source| RecordError::Read {
            path: path.clone(),
            source,
        })?
        .ok_or_else(|| RecordError::MissingHeader { path: path.clone() })?;

    let header: Header = serde_json::from_str(&first)
        .map_err(|_| RecordError::MissingHeader { path: path.clone() })?;

    if header.format != FORMAT || header.version != FORMAT_VERSION {
        return Err(RecordError::UnsupportedFormat {
            path,
            found: header.format,
            version: header.version,
        });
    }

    Ok((
        header,
        RecordReader {
            path,
            lines,
            line_number: 1,
        },
    ))
}

impl<R: BufRead> Iterator for RecordReader<R> {
    type Item = Result<Record, RecordError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(source) => {
                    return Some(Err(RecordError::Read {
                        path: self.path.clone(),
                        source,
                    }))
                }
            };
            self.line_number += 1;

            // A trailing newline is normal; a blank line in the middle is not
            // information either way.
            if line.trim().is_empty() {
                continue;
            }
            return Some(self.parse_line(&line));
        }
    }
}

impl<R> RecordReader<R> {
    /// The path being read, for error messages.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Data lines and marker lines are told apart by the presence of `marker`,
    /// rather than by serde's untagged fallback, so a malformed line reports
    /// what is actually wrong with it instead of "did not match any variant".
    ///
    /// The line is parsed **straight into the record type**, never via
    /// `serde_json::Value`. That is not a micro-optimisation: `Value`'s object
    /// is a `BTreeMap`, so round-tripping a payload through it silently sorts
    /// the keys and the archive stops being what Binance sent. Recording raw
    /// bytes is pointless if the reader normalises them on the way back out.
    /// The probe below looks only at `marker` and materialises nothing else.
    fn parse_line(&self, line: &str) -> Result<Record, RecordError> {
        #[derive(Deserialize)]
        struct Probe {
            #[serde(default)]
            marker: Option<String>,
        }

        let malformed = |reason: String| RecordError::MalformedRecord {
            path: self.path.clone(),
            line: self.line_number,
            reason,
        };

        let probe: Probe =
            serde_json::from_str(line).map_err(|e| malformed(format!("not a JSON object: {e}")))?;

        if probe.marker.is_some() {
            serde_json::from_str(line)
                .map(Record::Marker)
                .map_err(|e| malformed(format!("not a valid marker record: {e}")))
        } else {
            serde_json::from_str(line)
                .map(Record::Data)
                .map_err(|e| malformed(format!("not a valid data record: {e}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::normalize::SeqPolicy;
    use crate::GapKind;

    fn gap_detail() -> GapDetail {
        GapDetail {
            stream: "btcusdt@trade".to_owned(),
            policy: SeqPolicy::Contiguous,
            kind: GapKind::Missed,
            from: 100,
            to: 137,
            missing: Some(36),
        }
    }

    #[test]
    fn a_data_line_has_exactly_the_specified_shape_and_field_order() {
        // Pins the file format itself. This is what a backtester written against
        // the spec will parse, so a reordering or a renamed key is a break.
        let payload = RawValue::from_string(
            r#"{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000"}"#.to_owned(),
        )
        .expect("valid JSON");

        #[derive(Serialize)]
        struct DataLine<'a> {
            recv_ns: i64,
            seq: u64,
            stream: &'a str,
            payload: &'a RawValue,
        }

        let line = serde_json::to_string(&DataLine {
            recv_ns: 1_712_345_678_901_234_567,
            seq: 42,
            stream: "btcusdt@bookTicker",
            payload: &payload,
        })
        .expect("serialises");

        assert_eq!(
            line,
            r#"{"recv_ns":1712345678901234567,"seq":42,"stream":"btcusdt@bookTicker","payload":{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000"}}"#
        );
    }

    #[test]
    fn a_gap_marker_line_records_the_policy_that_applied() {
        let line = serde_json::to_string(&MarkerRecord {
            recv_ns: 1_712_345_678_901_234_567,
            seq: 43,
            marker: Marker::Gap(gap_detail()),
        })
        .expect("serialises");

        assert_eq!(
            line,
            r#"{"recv_ns":1712345678901234567,"seq":43,"marker":"gap","detail":{"stream":"btcusdt@trade","policy":"contiguous","kind":"missed","from":100,"to":137,"missing":36}}"#
        );
    }

    #[test]
    fn a_monotonic_gap_records_an_explicit_null_rather_than_an_absent_key() {
        // "We could not count" must not look like "we forgot to write it".
        let line = serde_json::to_string(&MarkerRecord {
            recv_ns: 1,
            seq: 0,
            marker: Marker::Gap(GapDetail {
                stream: "btcusdt@bookTicker".to_owned(),
                policy: SeqPolicy::Monotonic,
                kind: GapKind::Regressed,
                from: 500,
                to: 499,
                missing: None,
            }),
        })
        .expect("serialises");

        assert!(line.contains(r#""policy":"monotonic""#), "{line}");
        assert!(line.contains(r#""missing":null"#), "{line}");
    }

    #[test]
    fn disconnect_and_reconnect_markers_round_trip() {
        for marker in [
            Marker::Disconnect(DisconnectDetail {
                reason: "connection reset by peer".to_owned(),
            }),
            Marker::Reconnect(ReconnectDetail {
                attempt: 3,
                waited_ms: 1_800,
            }),
        ] {
            let record = MarkerRecord {
                recv_ns: 7,
                seq: 9,
                marker,
            };
            let text = serde_json::to_string(&record).expect("serialises");
            let back: MarkerRecord = serde_json::from_str(&text).expect("deserialises");
            assert_eq!(back, record, "{text}");
        }
    }

    #[test]
    fn markers_of_different_kinds_are_never_confused_for_one_another() {
        // A contiguous marker and a monotonic marker carry the same numbers and
        // mean different things; the reader must not be able to mix them up.
        let contiguous = MarkerRecord {
            recv_ns: 1,
            seq: 0,
            marker: Marker::Gap(GapDetail {
                policy: SeqPolicy::Contiguous,
                missing: Some(9),
                ..gap_detail()
            }),
        };
        let monotonic = MarkerRecord {
            recv_ns: 1,
            seq: 0,
            marker: Marker::Gap(GapDetail {
                policy: SeqPolicy::Monotonic,
                missing: None,
                ..gap_detail()
            }),
        };
        assert_ne!(contiguous, monotonic);

        let a = serde_json::to_string(&contiguous).expect("serialises");
        let b = serde_json::to_string(&monotonic).expect("serialises");
        assert_ne!(a, b);
        assert_eq!(
            serde_json::from_str::<MarkerRecord>(&a).expect("valid"),
            contiguous
        );
        assert_eq!(
            serde_json::from_str::<MarkerRecord>(&b).expect("valid"),
            monotonic
        );
    }

    #[test]
    fn a_marker_with_a_detail_of_the_wrong_shape_is_refused() {
        // Claiming to be a gap while carrying a disconnect's detail is a
        // corrupted line, not something to interpret hopefully.
        let text = r#"{"recv_ns":1,"seq":0,"marker":"gap","detail":{"reason":"nope"}}"#;
        let err = serde_json::from_str::<MarkerRecord>(text).expect_err("must refuse");
        assert!(err.to_string().contains("malformed detail"), "{err}");
    }

    // --- filename and timestamp formatting ---

    #[test]
    fn utc_stamps_are_correct_at_known_instants() {
        for (ns, expected) in [
            (0_i64, "19700101T000000Z"),
            (1_000_000_000, "19700101T000001Z"),
            // 2024-04-05T18:14:38.901234567Z - a leap year, past February.
            (1_712_340_878_901_234_567, "20240405T181438Z"),
            // 2000-02-29: the century leap year the naive rule gets wrong.
            (951_782_400_000_000_000, "20000229T000000Z"),
            // 2100-03-01: the century that is *not* a leap year.
            (4_107_542_400_000_000_000, "21000301T000000Z"),
            // 2023-12-31T23:59:59Z - the last second of a non-leap year.
            (1_704_067_199_000_000_000, "20231231T235959Z"),
        ] {
            assert_eq!(utc_stamp(ns), expected, "for {ns}ns");
        }
    }

    #[test]
    fn session_filenames_name_the_time_and_the_symbols() {
        let name = session_filename(
            1_712_340_878_901_234_567,
            &["BTCUSDT".to_owned(), "ETHUSDT".to_owned()],
        )
        .expect("valid");
        assert_eq!(name, "binance-20240405T181438Z-BTCUSDT_ETHUSDT");
    }

    #[test]
    fn a_long_symbol_set_is_summarised_rather_than_making_an_unusable_filename() {
        let symbols: Vec<String> = (0..20).map(|i| format!("SYM{i:02}USDT")).collect();
        let name = session_filename(0, &symbols).expect("valid");
        assert_eq!(name, "binance-19700101T000000Z-SYM00USDT_and19more");
        assert!(name.len() < 64, "{name}");
    }

    #[test]
    fn a_pre_epoch_start_time_is_refused() {
        assert!(matches!(
            session_filename(-1, &[]).expect_err("must refuse"),
            RecordError::InvalidStartTime { started_ns: -1 }
        ));
    }

    #[test]
    fn the_reader_refuses_a_file_with_no_header() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("headerless.ndjson");
        std::fs::write(
            &path,
            "{\"recv_ns\":1,\"seq\":0,\"stream\":\"a@trade\",\"payload\":{}}\n",
        )
        .expect("write");
        assert!(matches!(
            read_session(&path).expect_err("must refuse"),
            RecordError::MissingHeader { .. }
        ));
    }

    #[test]
    fn the_reader_refuses_a_format_or_version_it_does_not_know() {
        let dir = tempfile::tempdir().expect("temp dir");
        for (header, label) in [
            (
                json!({"format":"something-else","version":1,"started_ns":0,
                    "endpoint":"testnet","ws_url":"","symbols":[],"streams":[]}),
                "format",
            ),
            (
                json!({"format":FORMAT,"version":99,"started_ns":0,
                    "endpoint":"testnet","ws_url":"","symbols":[],"streams":[]}),
                "version",
            ),
        ] {
            let path = dir.path().join(format!("bad-{label}.ndjson"));
            std::fs::write(&path, format!("{header}\n")).expect("write");
            assert!(
                matches!(
                    read_session(&path).expect_err("must refuse"),
                    RecordError::UnsupportedFormat { .. }
                ),
                "unknown {label} must be refused"
            );
        }
    }

    #[test]
    fn a_malformed_line_is_a_typed_error_rather_than_a_silent_skip() {
        // A replay that quietly drops records is a backtest that quietly lies.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("torn.ndjson");
        let header = json!({"format":FORMAT,"version":FORMAT_VERSION,"started_ns":0,
                            "endpoint":"testnet","ws_url":"wss://x/ws","symbols":[],"streams":[]});
        std::fs::write(
            &path,
            format!("{header}\n{{\"recv_ns\":1,\"seq\":0,\"stream\":\"a@trade\"\n"),
        )
        .expect("write");

        let err = read_session(&path).expect_err("must refuse");
        assert!(
            matches!(&err, RecordError::MalformedRecord { line: 2, .. }),
            "got {err:?}"
        );
    }
}
