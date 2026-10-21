#[path = "group_commit/common.rs"]
mod common;

use common::{DATA, Outcome, Prepare, QueuedGroup, SyncFault, new_database, read};
#[cfg(panic = "unwind")]
use common::{OTHER, assert_success, spawn_commit};
#[cfg(all(panic = "unwind", feature = "experimental-api-5"))]
use redb::ReadableTable;
use redb::{ConcurrentWriteError, Database, GroupCommitConfig};
#[cfg(panic = "unwind")]
use redb::{Key, ReadableDatabase, TableDefinition, TypeName, Value};
#[cfg(panic = "unwind")]
use std::cmp::Ordering;
#[cfg(panic = "unwind")]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
#[cfg(panic = "unwind")]
use std::sync::mpsc;
#[cfg(panic = "unwind")]
use std::time::Duration;

fn insert(key: u64, value: u64) -> Prepare {
    Box::new(move |txn| {
        txn.open_table(DATA).unwrap().insert(key, value).unwrap();
    })
}

fn assert_batch_failed(result: Outcome) {
    match result {
        Err(_) => {}
        Ok(Err(ConcurrentWriteError::BatchAborted(cause))) => {
            assert!(matches!(cause.as_ref(), ConcurrentWriteError::Commit(_)));
        }
        Ok(Err(ConcurrentWriteError::BatchPanicked)) => {}
        other => panic!("expected a shared non-retryable failure, got {other:?}"),
    }
}

fn sync_failure(fault: SyncFault, durable: bool) {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let mut group = QueuedGroup::new(
        db.clone(),
        backend.clone(),
        vec![insert(1000, 11), insert(1001, 22)],
    );
    group.advance_to_batch(2);
    group.assert_followers_not_acknowledged();
    backend.fault_at(group.blocked_sync, fault);
    for result in group.finish() {
        assert_batch_failed(result);
    }
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.queued_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
    assert!(db.begin_write().is_err());
    // Capture the durable image before closing the failed handle can issue any cleanup.
    let mut recovered = Database::builder()
        .create_with_backend(backend.crash_image())
        .unwrap();
    for (key, value) in [(1000, 11), (1001, 22)] {
        assert_eq!(read(&recovered, DATA, key), durable.then_some(value));
    }
    assert!(recovered.check_integrity().unwrap());
}

#[test]
fn failed_sync_wakes_every_batch_caller_without_publishing_any_request() {
    sync_failure(SyncFault::ErrorBefore, false);
}

#[test]
fn failed_sync_can_already_have_persisted_the_whole_batch() {
    sync_failure(SyncFault::ErrorAfter, true);
}

#[cfg(panic = "unwind")]
#[test]
fn a_sync_panic_wakes_every_batch_caller_and_preserves_the_old_durable_state() {
    sync_failure(SyncFault::PanicBefore, false);
}

#[cfg(panic = "unwind")]
#[test]
fn a_sync_panic_after_persistence_has_an_uncertain_whole_batch_outcome() {
    sync_failure(SyncFault::PanicAfter, true);
}

#[cfg(panic = "unwind")]
static PANIC_COMPARE: AtomicBool = AtomicBool::new(false);

#[cfg(panic = "unwind")]
#[derive(Debug)]
struct PanicKey;

#[cfg(panic = "unwind")]
impl Value for PanicKey {
    type SelfType<'a> = u64;
    type AsBytes<'a> = [u8; 8];

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(bytes: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("group_commit_recovery::PanicKey")
    }
}

#[cfg(panic = "unwind")]
impl Key for PanicKey {
    fn compare(left: &[u8], right: &[u8]) -> Ordering {
        assert!(
            !PANIC_COMPARE.load(AtomicOrdering::Relaxed),
            "injected comparator panic"
        );
        u64::from_le_bytes(left.try_into().unwrap())
            .cmp(&u64::from_le_bytes(right.try_into().unwrap()))
    }
}

#[cfg(panic = "unwind")]
#[test]
fn a_comparator_panic_clears_handoff_and_does_not_strand_queued_callers() {
    const KEYS: TableDefinition<PanicKey, u64> = TableDefinition::new("panic_keys");
    let config = GroupCommitConfig::default().with_max_batch_transactions(2);
    let (db, backend) = new_database(config);
    let initial = db.begin_write().unwrap();
    initial.open_table(KEYS).unwrap().insert(1, 10).unwrap();
    initial.commit().unwrap();
    let prepare = || {
        Box::new(|txn: &mut redb::ConcurrentWriteTransaction<'_>| {
            let mut table = txn.open_table(KEYS).unwrap();
            assert_eq!(table.get(1).unwrap().unwrap().value(), 10);
            table.insert(1, 11).unwrap();
        }) as Prepare
    };
    let group = QueuedGroup::new(
        db.clone(),
        backend,
        vec![prepare(), prepare(), insert(1000, 1000)],
    );
    PANIC_COMPARE.store(true, AtomicOrdering::Relaxed);
    // The comparator is first called when the next batch checks intersections.
    // The third request remains queued and must also be released by leader unwind.
    group.paused.release_all();
    let results = group.finish();
    PANIC_COMPARE.store(false, AtomicOrdering::Relaxed);
    for result in results {
        match result {
            Err(_) | Ok(Err(ConcurrentWriteError::BatchPanicked)) => {}
            other => panic!("expected an unwound active or queued request, got {other:?}"),
        }
    }
    assert_eq!(read(&db, DATA, 1000), None);
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.queued_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
    // No replay happened, so the handle remains usable after poisoned history is cleared.
    let mut later = db.begin_concurrent_write().unwrap();
    later.open_table(OTHER).unwrap().insert(1, 111).unwrap();
    later.commit().unwrap();
    assert_eq!(read(&db, OTHER, 1), Some(111));
}

#[cfg(panic = "unwind")]
static PANIC_REPLAY: AtomicBool = AtomicBool::new(false);

#[cfg(panic = "unwind")]
#[derive(Debug)]
struct ReplayValue;

#[cfg(panic = "unwind")]
impl Value for ReplayValue {
    type SelfType<'a> = u64;
    type AsBytes<'a> = [u8; 8];

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(bytes: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        assert!(
            !PANIC_REPLAY.load(AtomicOrdering::Relaxed) || *value != 9001,
            "injected codec panic during batch replay"
        );
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("group_commit_recovery::ReplayValue")
    }
}

#[cfg(panic = "unwind")]
#[test]
fn a_later_replay_panic_does_not_publish_an_earlier_request() {
    const LATE: TableDefinition<u64, ReplayValue> = TableDefinition::new("late");
    let (db, backend) = new_database(GroupCommitConfig::default());
    let initial = db.begin_write().unwrap();
    initial.open_table(LATE).unwrap();
    initial.commit().unwrap();
    let group = QueuedGroup::new(
        db.clone(),
        backend.clone(),
        vec![
            insert(1000, 11),
            Box::new(|txn| {
                txn.open_table(LATE).unwrap().insert(7, 9001).unwrap();
            }),
        ],
    );
    PANIC_REPLAY.store(true, AtomicOrdering::Relaxed);
    for result in group.finish() {
        match result {
            Err(_) | Ok(Err(ConcurrentWriteError::BatchPanicked)) => {}
            other => panic!("expected batch panic, got {other:?}"),
        }
    }
    PANIC_REPLAY.store(false, AtomicOrdering::Relaxed);
    assert_eq!(read(&db, DATA, 1000), None);
    assert!(
        db.begin_read()
            .unwrap()
            .open_table(LATE)
            .unwrap()
            .get(7)
            .unwrap()
            .is_none()
    );
    let (completed, received) = mpsc::channel();
    let next = spawn_commit(db, 0, insert(1001, 22), completed);
    let (_, result) = received.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_success(result);
    next.join().unwrap();
    let mut recovered = Database::builder()
        .create_with_backend(backend.crash_image())
        .unwrap();
    assert_eq!(read(&recovered, DATA, 1000), None);
    assert_eq!(read(&recovered, DATA, 1001), Some(22));
    assert!(recovered.check_integrity().unwrap());
}

#[cfg(all(panic = "unwind", not(target_os = "wasi")))]
#[test]
fn a_backend_write_panic_does_not_strand_active_or_queued_callers() {
    const CHILD: &str = "REDB_GROUP_WRITE_PANIC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // A regression can double-panic while dropping a poisoned write-buffer lock.
        // Keep that abort isolated so the parent reports an ordinary test failure.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_backend_write_panic_does_not_strand_active_or_queued_callers",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "write-panic child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    const VALUE: u64 = 0x534f4d4556414c31;
    let config = GroupCommitConfig::default().with_max_batch_transactions(2);
    let (db, backend) = new_database(config);
    let group = QueuedGroup::new(
        db.clone(),
        backend.clone(),
        vec![insert(1000, VALUE), insert(1001, 22), insert(1002, 33)],
    );
    backend.panic_on_write_pattern(VALUE.to_le_bytes());
    for result in group.finish() {
        match result {
            Err(_) | Ok(Err(ConcurrentWriteError::BatchPanicked)) => {}
            other => panic!("expected batch panic, got {other:?}"),
        }
    }
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.active_transactions, 0);
    assert_eq!(stats.queued_transactions, 0);
    let mut recovered = Database::builder()
        .create_with_backend(backend.crash_image())
        .unwrap();
    for key in 1000..=1002 {
        assert_eq!(read(&recovered, DATA, key), None);
    }
    assert!(recovered.check_integrity().unwrap());
}
