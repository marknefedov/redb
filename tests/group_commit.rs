#[path = "group_commit/common.rs"]
mod common;

use common::{
    DATA, OTHER, Outcome, Prepare, QueuedGroup, assert_conflict, assert_success, new_database,
    read, spawn_commit, wait_for_stats,
};
#[cfg(feature = "experimental-api-5")]
use redb::ReadableTable;
use redb::{
    ConcurrentWriteError, Database, Durability, GroupCommitConfig, Key, ReadableDatabase,
    TableDefinition, TableError, TypeName, Value,
};
use std::cmp::Ordering;
use std::sync::mpsc;
use std::time::Duration;

fn insert(key: u64, value: u64) -> Prepare {
    Box::new(move |txn| {
        txn.open_table(DATA).unwrap().insert(key, value).unwrap();
    })
}

fn count_syncs(config: GroupCommitConfig, expected_group: usize) -> usize {
    let (db, backend) = new_database(config);
    let before = backend.syncs();
    let mut group = QueuedGroup::new(
        db.clone(),
        backend.clone(),
        vec![insert(1000, 11), insert(1001, 22), insert(1002, 33)],
    );
    group.advance_to_batch(expected_group);
    group.assert_followers_not_acknowledged();
    for result in group.finish() {
        assert_success(result);
    }
    for (key, value) in [(1000, 11), (1001, 22), (1002, 33)] {
        assert_eq!(read(&db, DATA, key), Some(value));
    }
    assert_eq!(
        db.group_commit_stats().unwrap().max_batch_transactions,
        expected_group
    );
    backend.syncs() - before
}

#[test]
fn queued_independent_transactions_share_syncs_and_wait_for_durability() {
    let serial = count_syncs(
        GroupCommitConfig::default().with_max_batch_transactions(1),
        1,
    );
    let grouped = count_syncs(GroupCommitConfig::default(), 3);
    assert!(grouped < serial, "grouped={grouped}, serial={serial}");
}

#[test]
fn conflicting_blind_writes_in_one_batch_have_one_fifo_winner() {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let mut group = QueuedGroup::new(
        db.clone(),
        backend,
        vec![insert(1000, 11), insert(1000, 22), insert(1000, 33)],
    );
    group.advance_to_batch(3);
    group.assert_followers_not_acknowledged();
    let mut results = group.finish().into_iter();
    assert_success(results.next().unwrap());
    assert_conflict(results.next().unwrap());
    assert_conflict(results.next().unwrap());
    assert_eq!(read(&db, DATA, 1000), Some(11));
}

#[test]
fn cross_transaction_read_dependencies_prevent_write_skew_within_a_batch() {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let jobs: Vec<Prepare> = (1..=2)
        .map(|key| {
            Box::new(move |txn: &mut redb::ConcurrentWriteTransaction<'_>| {
                let mut table = txn.open_table(DATA).unwrap();
                assert_eq!(table.get(1).unwrap().unwrap().value(), 10);
                assert_eq!(table.get(2).unwrap().unwrap().value(), 20);
                table.insert(key, 0).unwrap();
            }) as Prepare
        })
        .collect();
    let mut group = QueuedGroup::new(db.clone(), backend, jobs);
    group.advance_to_batch(2);
    let mut results = group.finish().into_iter();
    assert_success(results.next().unwrap());
    assert_conflict(results.next().unwrap());
    assert_eq!(read(&db, DATA, 1), Some(0));
    assert_eq!(read(&db, DATA, 2), Some(20));
}

#[test]
fn a_missing_read_conflicts_and_does_not_reject_later_independent_work() {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let jobs: Vec<Prepare> = vec![
        insert(1000, 11),
        Box::new(|txn| {
            assert!(txn.open_table(DATA).unwrap().get(1000).unwrap().is_none());
            txn.open_table(OTHER).unwrap().insert(1, 99).unwrap();
        }),
        Box::new(|txn| {
            assert!(txn.open_table(OTHER).unwrap().get(1).unwrap().is_none());
            txn.open_table(DATA).unwrap().insert(1001, 22).unwrap();
        }),
    ];
    let mut group = QueuedGroup::new(db.clone(), backend, jobs);
    group.advance_to_batch(3);
    let mut results = group.finish().into_iter();
    assert_success(results.next().unwrap());
    assert_conflict(results.next().unwrap());
    assert_success(results.next().unwrap());
    assert_eq!(read(&db, OTHER, 1), None);
    assert_eq!(read(&db, DATA, 1001), Some(22));
}

#[test]
fn table_wide_and_read_only_dependencies_are_checked_against_earlier_batch_writes() {
    for query in 0..4 {
        let (db, backend) = new_database(GroupCommitConfig::default());
        let jobs: Vec<Prepare> = vec![
            insert(1000, 11),
            Box::new(move |txn| {
                let mut table = txn.open_table(DATA).unwrap();
                match query {
                    0 => assert_eq!(table.len().unwrap(), 128),
                    1 => assert!(!table.is_empty().unwrap()),
                    2 => assert!(table.range(200..300).unwrap().next().is_none()),
                    3 => assert_eq!(table.iter().unwrap().count(), 128),
                    _ => unreachable!(),
                }
            }),
            insert(1001, 22),
        ];
        let mut group = QueuedGroup::new(db.clone(), backend, jobs);
        group.advance_to_batch(3);
        let mut results = group.finish().into_iter();
        assert_success(results.next().unwrap());
        assert_conflict(results.next().unwrap());
        assert_success(results.next().unwrap());
    }
}

#[test]
fn competing_table_creation_is_a_catalog_conflict_within_one_batch() {
    const NEW: TableDefinition<u64, u64> = TableDefinition::new("new");
    const WRONG: TableDefinition<&str, &str> = TableDefinition::new("new");
    let (db, backend) = new_database(GroupCommitConfig::default());
    let jobs: Vec<Prepare> = vec![
        Box::new(|txn| {
            txn.open_table(NEW).unwrap().insert(1, 11).unwrap();
        }),
        Box::new(|txn| {
            txn.open_table(NEW).unwrap().insert(2, 22).unwrap();
            assert!(matches!(
                txn.open_table(WRONG),
                Err(TableError::TableTypeMismatch { .. })
            ));
        }),
        insert(1001, 33),
    ];
    let mut group = QueuedGroup::new(db.clone(), backend, jobs);
    group.advance_to_batch(3);
    let mut results = group.finish().into_iter();
    assert_success(results.next().unwrap());
    assert_conflict(results.next().unwrap());
    assert_success(results.next().unwrap());
    assert_eq!(read(&db, NEW, 1), Some(11));
    assert_eq!(read(&db, NEW, 2), None);
}

#[derive(Debug)]
struct FoldedReverse;

impl Value for FoldedReverse {
    type SelfType<'a> = &'a str;
    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(bytes: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        std::str::from_utf8(bytes).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value.as_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("group_commit::FoldedReverse")
    }
}

impl Key for FoldedReverse {
    fn compare(left: &[u8], right: &[u8]) -> Ordering {
        left.iter()
            .map(u8::to_ascii_lowercase)
            .cmp(right.iter().map(u8::to_ascii_lowercase))
            .reverse()
    }
}

#[test]
fn batch_intersections_use_the_custom_key_comparator() {
    const STRINGS: TableDefinition<FoldedReverse, u64> = TableDefinition::new("strings");
    let (db, backend) = new_database(GroupCommitConfig::default());
    let initial = db.begin_write().unwrap();
    initial
        .open_table(STRINGS)
        .unwrap()
        .insert("alpha", 1)
        .unwrap();
    initial.commit().unwrap();
    let mut stale = db.begin_concurrent_write().unwrap();
    stale.open_table(STRINGS).unwrap().get("ALPHA").unwrap();
    stale.open_table(OTHER).unwrap().insert(2, 222).unwrap();
    let jobs: Vec<Prepare> = vec![
        Box::new(|txn| {
            txn.open_table(STRINGS).unwrap().insert("ALPHA", 2).unwrap();
        }),
        Box::new(|txn| {
            assert_eq!(
                txn.open_table(STRINGS)
                    .unwrap()
                    .get("alpha")
                    .unwrap()
                    .unwrap()
                    .value(),
                1
            );
            txn.open_table(OTHER).unwrap().insert(1, 99).unwrap();
        }),
        Box::new(|txn| {
            txn.open_table(STRINGS).unwrap().insert("Beta", 3).unwrap();
        }),
    ];
    let mut group = QueuedGroup::new(db.clone(), backend, jobs);
    group.advance_to_batch(3);
    let mut results = group.finish().into_iter();
    assert_success(results.next().unwrap());
    assert_conflict(results.next().unwrap());
    assert_success(results.next().unwrap());
    assert_eq!(read(&db, OTHER, 1), None);
    assert!(matches!(
        stale.commit(),
        Err(ConcurrentWriteError::Conflict { .. })
    ));
    assert_eq!(read(&db, OTHER, 2), None);
    let snapshot = db.begin_read().unwrap();
    let strings = snapshot.open_table(STRINGS).unwrap();
    assert_eq!(strings.get("alpha").unwrap().unwrap().value(), 2);
    assert_eq!(strings.get("BETA").unwrap().unwrap().value(), 3);
}

#[test]
fn a_full_queue_rejects_without_losing_the_accepted_requests() {
    let config = GroupCommitConfig::default().with_max_queue_transactions(1);
    let (db, backend) = new_database(config);
    let group = QueuedGroup::new(db.clone(), backend, vec![insert(1000, 11)]);
    let (completed, result) = mpsc::channel();
    let overflow = spawn_commit(db.clone(), 0, insert(1001, 22), completed);
    let (_, outcome) = result.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(matches!(
        outcome.unwrap(),
        Err(ConcurrentWriteError::QueueFull)
    ));
    overflow.join().unwrap();
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.queued_transactions, 1);
    assert_eq!(stats.max_queued_transactions, 1);
    for result in group.finish() {
        assert_success(result);
    }
    assert_eq!(read(&db, DATA, 1000), Some(11));
    assert_eq!(read(&db, DATA, 1001), None);
}

const BYTES: TableDefinition<u64, &[u8]> = TableDefinition::new("bytes");
const VALUE_BYTES: usize = 16 * 1024;

fn initialize_bytes(db: &Database) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(BYTES).unwrap();
        for key in 0..3 {
            table.insert(key, vec![1; VALUE_BYTES].as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
}

fn replace_bytes(key: u64) -> Prepare {
    Box::new(move |txn| {
        let mut table = txn.open_table(BYTES).unwrap();
        assert_eq!(table.get(key).unwrap().unwrap().value().len(), VALUE_BYTES);
        table.insert(key, vec![2; VALUE_BYTES].as_slice()).unwrap();
    })
}

fn prepared_byte_charge() -> usize {
    let (db, backend) = new_database(GroupCommitConfig::default());
    initialize_bytes(&db);
    let group = QueuedGroup::new(db.clone(), backend, vec![replace_bytes(0)]);
    let bytes = db.group_commit_stats().unwrap().queued_bytes;
    assert!(
        bytes >= VALUE_BYTES * 2,
        "original and staged values must both count"
    );
    for result in group.finish() {
        assert_success(result);
    }
    bytes
}

#[test]
fn batch_byte_limit_bounds_retained_original_and_staged_values() {
    let bytes = prepared_byte_charge();
    let config = GroupCommitConfig::default()
        .with_max_batch_bytes(bytes)
        .with_max_queue_bytes(bytes * 4);
    let (db, backend) = new_database(config);
    initialize_bytes(&db);
    let mut group = QueuedGroup::new(
        db.clone(),
        backend,
        vec![replace_bytes(0), replace_bytes(1)],
    );
    group.advance_to_batch(1);
    assert_eq!(db.group_commit_stats().unwrap().active_transactions, 1);
    for result in group.finish() {
        assert_success(result);
    }
    let stats = db.group_commit_stats().unwrap();
    assert!(stats.max_batch_bytes <= bytes);
    assert_eq!(stats.physical_batches, 3);
    let snapshot = db.begin_read().unwrap();
    let table = snapshot.open_table(BYTES).unwrap();
    for key in 0..2 {
        assert_eq!(
            table.get(key).unwrap().unwrap().value(),
            vec![2; VALUE_BYTES]
        );
    }
}

#[test]
fn queue_byte_limit_rejects_before_retaining_another_prepared_request() {
    let bytes = prepared_byte_charge();
    let config = GroupCommitConfig::default()
        .with_max_batch_bytes(bytes)
        .with_max_queue_bytes(bytes);
    let (db, backend) = new_database(config);
    initialize_bytes(&db);
    let group = QueuedGroup::new(db.clone(), backend, vec![replace_bytes(0)]);
    let (completed, result) = mpsc::channel();
    let overflow = spawn_commit(db.clone(), 0, replace_bytes(1), completed);
    let (_, outcome) = result.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(matches!(
        outcome.unwrap(),
        Err(ConcurrentWriteError::QueueFull)
    ));
    overflow.join().unwrap();
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.queued_bytes, bytes);
    assert!(stats.max_queued_bytes <= bytes);
    for result in group.finish() {
        assert_success(result);
    }
}

#[test]
fn oversized_prepared_requests_use_the_ordinary_commit_path() {
    let (db, backend) = new_database(GroupCommitConfig::default().with_max_batch_bytes(1));
    initialize_bytes(&db);
    let before = backend.syncs();
    let mut txn = db.begin_concurrent_write().unwrap();
    replace_bytes(0)(&mut txn);
    txn.commit().unwrap();
    assert!(backend.syncs() > before);
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.physical_batches, 0);
    assert_eq!(stats.max_queued_bytes, 0);
    assert_eq!(stats.max_batch_bytes, 0);
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(BYTES)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value(),
        vec![2; VALUE_BYTES]
    );
}

#[test]
fn grouped_history_and_classic_commit_gaps_preserve_preimage_validation() {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let mut disjoint = db.begin_concurrent_write().unwrap();
    disjoint.open_table(DATA).unwrap().get(1).unwrap();
    disjoint.open_table(OTHER).unwrap().insert(1, 111).unwrap();
    let mut overlapping = db.begin_concurrent_write().unwrap();
    overlapping.open_table(DATA).unwrap().get(2).unwrap();
    overlapping
        .open_table(OTHER)
        .unwrap()
        .insert(2, 222)
        .unwrap();
    let group = QueuedGroup::new(db.clone(), backend, vec![insert(2, 2000), insert(3, 3000)]);
    for result in group.finish() {
        assert_success(result);
    }
    assert!(matches!(
        overlapping.commit(),
        Err(ConcurrentWriteError::Conflict { .. })
    ));
    let classic = db.begin_write().unwrap();
    classic.open_table(DATA).unwrap().insert(4, 4000).unwrap();
    classic.commit().unwrap();
    disjoint.commit().unwrap();
    assert_eq!(read(&db, OTHER, 1), Some(111));
    assert_eq!(read(&db, OTHER, 2), None);
}

#[test]
fn handled_schema_error_conflicts_after_a_classic_rewrite_and_handoff_continues() {
    const REQUESTED: TableDefinition<u64, u64> = TableDefinition::new("schema");
    const WRONG: TableDefinition<&str, &str> = TableDefinition::new("schema");
    let (db, _) = new_database(GroupCommitConfig::default());
    let initial = db.begin_write().unwrap();
    initial.open_table(WRONG).unwrap();
    initial.commit().unwrap();
    let classic = db.begin_write().unwrap();
    let (completed, results) = mpsc::channel();
    let first = spawn_commit(
        db.clone(),
        0,
        Box::new(|txn| {
            assert!(matches!(
                txn.open_table(REQUESTED),
                Err(TableError::TableTypeMismatch { .. })
            ));
            txn.open_table(OTHER).unwrap().insert(1, 111).unwrap();
        }),
        completed.clone(),
    );
    wait_for_stats(&db, |stats| stats.active_transactions == 1);
    let second = spawn_commit(db.clone(), 1, insert(1000, 22), completed);
    wait_for_stats(&db, |stats| stats.queued_transactions == 1);
    classic.delete_table(WRONG).unwrap();
    classic
        .open_table(REQUESTED)
        .unwrap()
        .insert(1, 11)
        .unwrap();
    classic.commit().unwrap();
    let mut completed: Vec<(usize, Outcome)> = (0..2)
        .map(|_| results.recv_timeout(Duration::from_secs(30)).unwrap())
        .collect();
    completed.sort_by_key(|(id, _)| *id);
    let mut completed = completed.into_iter().map(|(_, result)| result);
    assert_conflict(completed.next().unwrap());
    assert_success(completed.next().unwrap());
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(read(&db, OTHER, 1), None);
    assert_eq!(read(&db, DATA, 1000), Some(22));
}

#[test]
fn an_oversized_grouped_history_union_falls_back_to_actual_preimages() {
    const KEYS: TableDefinition<&[u8], u64> = TableDefinition::new("keys");
    let (db, backend) = new_database(GroupCommitConfig::default());
    let initial = db.begin_write().unwrap();
    {
        let mut table = initial.open_table(KEYS).unwrap();
        table.insert(b"watched".as_slice(), 1).unwrap();
        table.insert(b"unchanged".as_slice(), 7).unwrap();
    }
    initial.commit().unwrap();
    let mut overlapping = db.begin_concurrent_write().unwrap();
    overlapping
        .open_table(KEYS)
        .unwrap()
        .get(b"watched".as_slice())
        .unwrap();
    overlapping
        .open_table(OTHER)
        .unwrap()
        .insert(1, 111)
        .unwrap();
    let mut disjoint = db.begin_concurrent_write().unwrap();
    disjoint
        .open_table(KEYS)
        .unwrap()
        .get(b"unchanged".as_slice())
        .unwrap();
    disjoint.open_table(OTHER).unwrap().insert(2, 222).unwrap();
    let jobs: Vec<Prepare> = vec![
        Box::new(|txn| {
            txn.open_table(KEYS)
                .unwrap()
                .insert(b"watched".as_slice(), 2)
                .unwrap();
        }),
        Box::new(|txn| {
            txn.open_table(KEYS)
                .unwrap()
                .insert(vec![0xFF; 70 * 1024].as_slice(), 3)
                .unwrap();
        }),
    ];
    let mut group = QueuedGroup::new(db.clone(), backend, jobs);
    group.advance_to_batch(2);
    for result in group.finish() {
        assert_success(result);
    }
    assert!(matches!(
        overlapping.commit(),
        Err(ConcurrentWriteError::Conflict { .. })
    ));
    disjoint.commit().unwrap();
    assert_eq!(read(&db, OTHER, 1), None);
    assert_eq!(read(&db, OTHER, 2), Some(222));
}

#[test]
fn a_read_only_batch_succeeds_without_an_extra_publication_and_hands_off() {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let before = backend.syncs();
    let jobs: Vec<Prepare> = (1..=2)
        .map(|key| {
            Box::new(move |txn: &mut redb::ConcurrentWriteTransaction<'_>| {
                assert_eq!(
                    txn.open_table(DATA)
                        .unwrap()
                        .get(key)
                        .unwrap()
                        .unwrap()
                        .value(),
                    key * 10
                );
            }) as Prepare
        })
        .collect();
    let group = QueuedGroup::new(db.clone(), backend.clone(), jobs);
    for result in group.finish() {
        assert_success(result);
    }
    assert_eq!(backend.syncs() - before, 1);
    let stats = db.group_commit_stats().unwrap();
    assert_eq!(stats.physical_batches, 1);
    assert_eq!(stats.committed_transactions, 3);
    assert_eq!(stats.queued_transactions, 0);
    assert_eq!(stats.active_transactions, 0);
    let mut later = db.begin_concurrent_write().unwrap();
    later.open_table(DATA).unwrap().insert(1000, 111).unwrap();
    later.commit().unwrap();
    assert_eq!(read(&db, DATA, 1000), Some(111));
}

#[test]
fn classic_and_non_durable_commits_coexist_with_pinned_grouped_snapshots() {
    let (db, backend) = new_database(GroupCommitConfig::default());
    let pinned = db.begin_read().unwrap();
    let mut classic = db.begin_write().unwrap();
    classic.set_durability(Durability::None).unwrap();
    classic.open_table(DATA).unwrap().insert(1, 1000).unwrap();
    classic.commit().unwrap();
    let mut optimistic = db.begin_concurrent_write().unwrap();
    optimistic.set_durability(Durability::None).unwrap();
    optimistic
        .open_table(DATA)
        .unwrap()
        .insert(2, 2000)
        .unwrap();
    optimistic.commit().unwrap();
    assert_eq!(db.group_commit_stats().unwrap().physical_batches, 0);
    let group = QueuedGroup::new(
        db.clone(),
        backend.clone(),
        vec![insert(3, 3000), insert(4, 4000)],
    );
    for result in group.finish() {
        assert_success(result);
    }
    let old = pinned.open_table(DATA).unwrap();
    for key in 1..=4 {
        assert_eq!(old.get(key).unwrap().unwrap().value(), key * 10);
    }
    drop(old);
    drop(pinned);
    let mut recovered = Database::builder()
        .create_with_backend(backend.crash_image())
        .unwrap();
    for key in 1..=4 {
        assert_eq!(read(&recovered, DATA, key), Some(key * 1000));
    }
    assert!(recovered.check_integrity().unwrap());
}
