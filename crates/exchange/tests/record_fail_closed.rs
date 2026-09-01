//! Recording enabled + unwritable target must **refuse to start**.
//!
//! This is the one failure that quietly destroys the archive. A bot that cannot
//! open its recording and keeps trading produces no error, no gap, and no way to
//! tell afterwards which sessions were captured and which were not - you find
//! out months later when a backtest has nothing to replay. So the refusal is a
//! startup error, and it is proven here rather than assumed.
//!
//! Two kinds of unwritable are exercised. The first (a file where the directory
//! should be) fails for every user including root, so it always runs. The second
//! (a read-only directory) is the realistic deployment case, and is skipped -
//! loudly - if the process turns out to be able to write there anyway, which is
//! what happens under root. A test that silently passes because it could not
//! create the condition it tests is worse than no test.

use std::path::{Path, PathBuf};

use exchange::{EndpointClass, RecordError, Recorder, RecorderConfig};

const START_NS: i64 = 1_712_340_878_000_000_000;

fn config(dir: &Path) -> RecorderConfig {
    RecorderConfig {
        dir: dir.to_path_buf(),
        started_ns: START_NS,
        endpoint: EndpointClass::Testnet,
        ws_url: "wss://stream.testnet.binance.vision/ws".to_owned(),
        symbols: vec!["BTCUSDT".to_owned()],
        streams: vec!["btcusdt@trade".to_owned()],
    }
}

/// Can this process create a file in `dir` despite the permissions we set?
///
/// Root can, which would make the read-only-directory tests pass vacuously.
fn can_still_write(dir: &Path) -> bool {
    let probe = dir.join(".write-probe");
    let writable = std::fs::write(&probe, b"x").is_ok();
    let _ = std::fs::remove_file(&probe);
    writable
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .expect("should be able to change permissions on a temp dir");
}

#[test]
fn a_file_where_the_directory_should_be_refuses_to_start() {
    // Root-proof: `create_dir_all` cannot succeed over an existing regular file
    // for any user, so this case always actually runs.
    let dir = tempfile::tempdir().expect("temp dir");
    let blocker = dir.path().join("recordings");
    std::fs::write(&blocker, b"not a directory").expect("write");

    let err = Recorder::create(&config(&blocker)).expect_err("must refuse to start");
    assert!(
        matches!(&err, RecordError::DirUnusable { .. }),
        "got {err:?}"
    );

    let message = err.to_string();
    assert!(
        message.contains("refusing to start"),
        "the operator must be told this is a refusal, not a warning: {message}"
    );
    assert!(
        message.contains("unrecorded"),
        "the message must name the thing being prevented: {message}"
    );
}

#[test]
fn a_nested_path_under_a_file_refuses_to_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    let blocker = dir.path().join("afile");
    std::fs::write(&blocker, b"x").expect("write");

    let err = Recorder::create(&config(&blocker.join("nested/deeper")))
        .expect_err("must refuse to start");
    assert!(
        matches!(err, RecordError::DirUnusable { .. }),
        "got {err:?}"
    );
}

#[cfg(unix)]
#[test]
fn a_read_only_directory_refuses_to_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    let recordings = dir.path().join("recordings");
    std::fs::create_dir(&recordings).expect("create");

    set_mode(&recordings, 0o555);

    if can_still_write(&recordings) {
        // Restore before bailing, so the temp dir can be cleaned up.
        set_mode(&recordings, 0o755);
        eprintln!(
            "SKIPPED a_read_only_directory_refuses_to_start: this process can write to a \
             0555 directory (running as root?), so the condition under test does not hold here"
        );
        return;
    }

    let result = Recorder::create(&config(&recordings));
    set_mode(&recordings, 0o755);

    let err = result
        .map(|r| r.path().to_path_buf())
        .expect_err("must refuse to start when it cannot open the session file");
    assert!(
        matches!(&err, RecordError::FileUnopenable { .. }),
        "got {err:?}"
    );
    assert!(err.to_string().contains("refusing to start"), "{err}");
}

#[cfg(unix)]
#[test]
fn nothing_is_left_behind_when_recording_refuses_to_start() {
    // A refusal must not leave a half-open or empty session file that a later
    // reader would mistake for a real capture.
    let dir = tempfile::tempdir().expect("temp dir");
    let recordings = dir.path().join("recordings");
    std::fs::create_dir(&recordings).expect("create");
    set_mode(&recordings, 0o555);

    if can_still_write(&recordings) {
        set_mode(&recordings, 0o755);
        eprintln!(
            "SKIPPED nothing_is_left_behind_when_recording_refuses_to_start: running as root?"
        );
        return;
    }

    let result = Recorder::create(&config(&recordings));
    set_mode(&recordings, 0o755);
    assert!(result.is_err(), "must refuse");

    let entries: Vec<_> = std::fs::read_dir(&recordings)
        .expect("readable")
        .map(|e| e.expect("entry").path())
        .collect();
    assert!(entries.is_empty(), "left files behind: {entries:?}");
}

#[test]
fn writability_is_proven_at_startup_not_at_the_first_message() {
    // The header is written and flushed by `create`, so a target that cannot be
    // written to fails while the process can still refuse to run. If this were
    // lazy, the failure would land at the first book update - long after the
    // operator saw "started" in the log.
    let dir = tempfile::tempdir().expect("temp dir");
    let recorder = Recorder::create(&config(dir.path())).expect("usable target");
    let path = recorder.path().to_path_buf();

    let on_disk = std::fs::read_to_string(&path).expect("the header should already be flushed");
    assert!(
        on_disk.lines().count() == 1,
        "exactly the header, before any payload: {on_disk:?}"
    );
    assert!(on_disk.contains(exchange::FORMAT), "{on_disk}");
    assert_eq!(recorder.next_seq(), 0, "the header consumes no ingest seq");
}

#[test]
fn an_existing_session_file_is_never_appended_to_or_overwritten() {
    // Two sessions must never interleave in one file, and history is never
    // clobbered. A same-second restart gets its own file instead.
    let dir = tempfile::tempdir().expect("temp dir");
    let config = config(dir.path());

    let first = Recorder::create(&config).expect("first session");
    let first_path = first.path().to_path_buf();
    let first_contents = std::fs::read_to_string(&first_path).expect("readable");
    drop(first);

    let second = Recorder::create(&config).expect("second session, same second");
    let second_path = second.path().to_path_buf();

    assert_ne!(first_path, second_path, "must not reuse the filename");
    assert_eq!(
        std::fs::read_to_string(&first_path).expect("readable"),
        first_contents,
        "the earlier session's file must be untouched"
    );
    assert_eq!(
        second_path.file_name().and_then(|n| n.to_str()),
        Some("binance-20240405T181438Z-BTCUSDT-1.ndjson")
    );
}

#[test]
fn a_missing_directory_is_created_rather_than_refused() {
    // Fail-closed is about refusing what we cannot do, not about refusing work
    // we can do perfectly well. A first run on a fresh host should record.
    let dir = tempfile::tempdir().expect("temp dir");
    let nested: PathBuf = dir.path().join("a/b/c");
    assert!(!nested.exists());

    let recorder = Recorder::create(&config(&nested)).expect("should create the path");
    assert!(recorder.path().starts_with(&nested));
    assert!(nested.is_dir());
}

#[test]
fn a_pre_epoch_session_start_refuses_to_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut config = config(dir.path());
    config.started_ns = -1;

    assert!(matches!(
        Recorder::create(&config).expect_err("must refuse"),
        RecordError::InvalidStartTime { started_ns: -1 }
    ));
    assert_eq!(
        std::fs::read_dir(dir.path()).expect("readable").count(),
        0,
        "a refused session must not create anything"
    );
}
