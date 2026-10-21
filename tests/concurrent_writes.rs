use redb::{
    ConcurrentWriteError, Database, Key, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    StorageBackend, TableDefinition, TypeName, Value,
};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

const FIRST: TableDefinition<u64, u64> = TableDefinition::new("first");
const SECOND: TableDefinition<u64, u64> = TableDefinition::new("second");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn populate(db: &Database) {
    let txn = db.begin_write().unwrap();
    for definition in [FIRST, SECOND] {
        let mut table = txn.open_table(definition).unwrap();
        table.insert(1, 10).unwrap();
        table.insert(2, 20).unwrap();
    }
    txn.commit().unwrap();
}

fn get(db: &Database, definition: TableDefinition<u64, u64>, key: u64) -> Option<u64> {
    db.begin_read()
        .unwrap()
        .open_table(definition)
        .unwrap()
        .get(key)
        .unwrap()
        .map(|value| value.value())
}

fn assert_conflict(result: Result<(), ConcurrentWriteError>) {
    assert!(matches!(result, Err(ConcurrentWriteError::Conflict { .. })));
}

#[cfg(not(target_os = "wasi"))]
fn commit_prepared_pair(
    db: &Database,
    left: (TableDefinition<u64, u64>, u64),
    right: (TableDefinition<u64, u64>, u64),
) -> (
    Result<(), ConcurrentWriteError>,
    Result<(), ConcurrentWriteError>,
) {
    use std::sync::{Barrier, mpsc};
    use std::time::Duration;

    let start = Barrier::new(2);
    let (prepared, ready) = mpsc::channel();
    let (release_left, left_released) = mpsc::channel();
    let (release_right, right_released) = mpsc::channel();
    std::thread::scope(|scope| {
        let prepare = |(definition, key): (TableDefinition<u64, u64>, u64),
                       released: mpsc::Receiver<()>| {
            start.wait();
            let mut txn = db.begin_concurrent_write().unwrap();
            {
                let mut table = txn.open_table(definition).unwrap();
                assert!(table.get(key).unwrap().is_none());
                table.insert(key, key * 10).unwrap();
            }
            prepared.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(30)).unwrap();
            txn.commit()
        };
        let left_worker = scope.spawn(move || prepare(left, left_released));
        let right_worker = scope.spawn(move || prepare(right, right_released));
        let left_ready = ready.recv_timeout(Duration::from_secs(10));
        let right_ready = ready.recv_timeout(Duration::from_secs(10));
        // Release even on a failed rendezvous, so a regression which takes the classic
        // writer slot during preparation can unwind instead of hanging the test suite.
        release_left.send(()).unwrap();
        release_right.send(()).unwrap();
        let left_result = left_worker.join().unwrap();
        let right_result = right_worker.join().unwrap();
        assert!(left_ready.is_ok() && right_ready.is_ok());
        (left_result, right_result)
    })
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn independent_tables_prepare_and_commit_concurrently() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let (left, right) = commit_prepared_pair(&db, (FIRST, 100), (SECOND, 200));
    left.unwrap();
    right.unwrap();
    assert_eq!(get(&db, FIRST, 100), Some(1000));
    assert_eq!(get(&db, SECOND, 200), Some(2000));
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn independent_keys_in_one_table_prepare_and_commit_concurrently() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let (left, right) = commit_prepared_pair(&db, (FIRST, 100), (FIRST, 200));
    left.unwrap();
    right.unwrap();
    assert_eq!(get(&db, FIRST, 100), Some(1000));
    assert_eq!(get(&db, FIRST, 200), Some(2000));
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn simultaneous_writes_to_one_key_have_exactly_one_winner() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let (left, right) = commit_prepared_pair(&db, (FIRST, 100), (FIRST, 100));
    match (left, right) {
        (Ok(()), conflict) | (conflict, Ok(())) => assert_conflict(conflict),
        other => panic!("expected one successful commit and one conflict, got {other:?}"),
    }
    assert_eq!(get(&db, FIRST, 100), Some(1000));
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn seeded_counter_updates_retry_fresh_snapshots_without_lost_writes() {
    use redb::backends::InMemoryBackend;
    use std::sync::Barrier;

    const COUNTERS: TableDefinition<u64, u64> = TableDefinition::new("counters");
    const THREADS: usize = 4;
    const OPERATIONS: usize = 64;
    const TOTAL: u64 = 100;

    fn next_key(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*state >> 32) % 8
    }

    let mut db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let initial = db.begin_write().unwrap();
    {
        let mut table = initial.open_table(COUNTERS).unwrap();
        for key in (0..8).chain(std::iter::once(TOTAL)) {
            table.insert(key, 0).unwrap();
        }
    }
    initial.commit().unwrap();
    // Pin the same initial snapshot for every first attempt, forcing real conflicts
    // regardless of scheduling. Every retry must begin a new transaction.
    let prepared: Vec<_> = (0..THREADS)
        .map(|_| db.begin_concurrent_write().unwrap())
        .collect();
    let start = Barrier::new(THREADS);
    let conflicts: usize = std::thread::scope(|scope| {
        let db = &db;
        let start = &start;
        let workers: Vec<_> = prepared
            .into_iter()
            .enumerate()
            .map(|(worker, txn)| {
                scope.spawn(move || {
                    let mut first = Some(txn);
                    let mut seed = u64::try_from(worker).unwrap() + 1;
                    let mut conflicts = 0;
                    start.wait();
                    for _ in 0..OPERATIONS {
                        let key = next_key(&mut seed);
                        let mut retries = 0;
                        loop {
                            let mut txn = first
                                .take()
                                .unwrap_or_else(|| db.begin_concurrent_write().unwrap());
                            {
                                let mut table = txn.open_table(COUNTERS).unwrap();
                                let value = table.get(key).unwrap().unwrap().value();
                                let total = table.get(TOTAL).unwrap().unwrap().value();
                                table.insert(key, value + 1).unwrap();
                                table.insert(TOTAL, total + 1).unwrap();
                            }
                            match txn.commit() {
                                Ok(()) => break,
                                Err(ConcurrentWriteError::Conflict { .. }) => {
                                    retries += 1;
                                    conflicts += 1;
                                    assert!(retries < 1000, "retry loop made no progress");
                                    std::thread::yield_now();
                                }
                                Err(error) => panic!("unexpected commit error: {error}"),
                            }
                        }
                    }
                    conflicts
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum()
    });
    assert!(conflicts >= THREADS - 1);
    let mut expected = [0; 8];
    for worker in 0..THREADS {
        let mut seed = u64::try_from(worker).unwrap() + 1;
        for _ in 0..OPERATIONS {
            expected[usize::try_from(next_key(&mut seed)).unwrap()] += 1;
        }
    }
    let read = db.begin_read().unwrap();
    let table = read.open_table(COUNTERS).unwrap();
    for (key, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            table
                .get(u64::try_from(key).unwrap())
                .unwrap()
                .unwrap()
                .value(),
            expected
        );
    }
    assert_eq!(
        table.get(TOTAL).unwrap().unwrap().value(),
        u64::try_from(THREADS * OPERATIONS).unwrap()
    );
    drop(table);
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn competing_writes_conflict_without_overwriting_the_winner() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut first = db.begin_concurrent_write().unwrap();
    let mut second = db.begin_concurrent_write().unwrap();
    first.open_table(FIRST).unwrap().insert(1, 111).unwrap();
    second.open_table(FIRST).unwrap().insert(1, 222).unwrap();
    first.commit().unwrap();
    assert_conflict(second.commit());
    assert_eq!(get(&db, FIRST, 1), Some(111));
}

#[test]
fn reading_an_absent_key_protects_against_an_insert() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut reader = db.begin_concurrent_write().unwrap();
    {
        let mut table = reader.open_table(FIRST).unwrap();
        assert!(table.get(100).unwrap().is_none());
        table.insert(200, 2000).unwrap();
    }
    let writer = db.begin_write().unwrap();
    writer.open_table(FIRST).unwrap().insert(100, 1000).unwrap();
    writer.commit().unwrap();
    assert_conflict(reader.commit());
    assert_eq!(get(&db, FIRST, 100), Some(1000));
    assert_eq!(get(&db, FIRST, 200), None);
}

#[test]
fn read_dependencies_prevent_write_skew() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut first = db.begin_concurrent_write().unwrap();
    let mut second = db.begin_concurrent_write().unwrap();
    {
        let mut table = first.open_table(FIRST).unwrap();
        assert_eq!(table.get(2).unwrap().unwrap().value(), 20);
        table.insert(1, 0).unwrap();
    }
    {
        let mut table = second.open_table(FIRST).unwrap();
        assert_eq!(table.get(1).unwrap().unwrap().value(), 10);
        table.insert(2, 0).unwrap();
    }
    first.commit().unwrap();
    assert_conflict(second.commit());
    assert_eq!(get(&db, FIRST, 1), Some(0));
    assert_eq!(get(&db, FIRST, 2), Some(20));
}

#[test]
fn range_reads_detect_phantom_keys() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut reader = db.begin_concurrent_write().unwrap();
    {
        let mut table = reader.open_table(FIRST).unwrap();
        assert!(table.range(10..20).unwrap().next().is_none());
        table.insert(100, 1000).unwrap();
    }
    let writer = db.begin_write().unwrap();
    writer.open_table(FIRST).unwrap().insert(15, 150).unwrap();
    writer.commit().unwrap();
    assert_conflict(reader.commit());
    assert_eq!(get(&db, FIRST, 100), None);
}

#[test]
fn length_and_empty_reads_detect_phantoms() {
    const EMPTY: TableDefinition<u64, u64> = TableDefinition::new("empty");
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let writer = db.begin_write().unwrap();
    writer.open_table(EMPTY).unwrap();
    writer.commit().unwrap();
    for (definition, expected_len) in [(FIRST, 2), (EMPTY, 0)] {
        let mut reader = db.begin_concurrent_write().unwrap();
        {
            let mut table = reader.open_table(definition).unwrap();
            if expected_len == 0 {
                assert!(table.is_empty().unwrap());
            } else {
                assert_eq!(table.len().unwrap(), expected_len);
            }
        }
        reader
            .open_table(SECOND)
            .unwrap()
            .insert(100, 1000)
            .unwrap();
        let writer = db.begin_write().unwrap();
        writer
            .open_table(definition)
            .unwrap()
            .insert(50, 500)
            .unwrap();
        writer.commit().unwrap();
        assert_conflict(reader.commit());
        assert_eq!(get(&db, SECOND, 100), None);
    }
}

#[test]
fn staged_values_and_ranges_own_their_data() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut txn = db.begin_concurrent_write().unwrap();
    let (original, range) = {
        let mut table = txn.open_table(FIRST).unwrap();
        let original = table.insert(1, 11).unwrap().unwrap();
        assert_eq!(original.value(), 10);
        assert_eq!(table.remove(1).unwrap().unwrap().value(), 11);
        assert!(table.get(1).unwrap().is_none());
        assert!(table.remove(1).unwrap().is_none());
        assert!(table.insert(1, 12).unwrap().is_none());
        assert_eq!(table.remove(2).unwrap().unwrap().value(), 20);
        table.insert(3, 30).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        assert!(!table.is_empty().unwrap());
        (original, table.iter().unwrap())
    };
    txn.abort();
    assert_eq!(original.value(), 10);
    let contents: Vec<_> = range
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value(), value.value())
        })
        .collect();
    assert_eq!(contents, [(1, 12), (3, 30)]);
    assert_eq!(get(&db, FIRST, 1), Some(10));
    assert_eq!(get(&db, FIRST, 2), Some(20));
    assert_eq!(get(&db, FIRST, 3), None);
}

#[derive(Debug)]
struct FoldedReverseKey;

impl Value for FoldedReverseKey {
    type SelfType<'a> = &'a str;
    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        value.as_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("concurrent_writes::FoldedReverseKey")
    }
}

impl Key for FoldedReverseKey {
    fn compare(left: &[u8], right: &[u8]) -> Ordering {
        right
            .iter()
            .map(u8::to_ascii_lowercase)
            .cmp(left.iter().map(u8::to_ascii_lowercase))
    }
}

#[test]
fn custom_comparator_and_borrowed_values_work_in_the_overlay_and_conflict_check() {
    const STRINGS: TableDefinition<FoldedReverseKey, &str> = TableDefinition::new("strings");
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let writer = db.begin_write().unwrap();
    {
        let mut table = writer.open_table(STRINGS).unwrap();
        table.insert("Alpha", "before").unwrap();
        table.insert("Gamma", "last").unwrap();
    }
    writer.commit().unwrap();
    let mut txn = db.begin_concurrent_write().unwrap();
    {
        let mut table = txn.open_table(STRINGS).unwrap();
        {
            let key = String::from("ALPHA");
            let value = String::from("owned by the transaction");
            assert_eq!(
                table
                    .insert(key.as_str(), value.as_str())
                    .unwrap()
                    .unwrap()
                    .value(),
                "before"
            );
        }
        assert_eq!(
            table.get("alpha").unwrap().unwrap().value(),
            "owned by the transaction"
        );
        table.insert("Beta", "middle").unwrap();
        assert_eq!(table.len().unwrap(), 3);
        let values: Vec<_> = table
            .range("gamma"..="alpha")
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_ascii_lowercase(), value.value().to_owned())
            })
            .collect();
        assert_eq!(
            values,
            [
                (String::from("gamma"), String::from("last")),
                (String::from("beta"), String::from("middle")),
                (
                    String::from("alpha"),
                    String::from("owned by the transaction")
                ),
            ]
        );
    }
    txn.commit().unwrap();
    let mut reader = db.begin_concurrent_write().unwrap();
    {
        let mut table = reader.open_table(STRINGS).unwrap();
        assert!(table.get("aLpHa").unwrap().is_some());
        table.insert("Delta", "not committed").unwrap();
    }
    let writer = db.begin_write().unwrap();
    writer
        .open_table(STRINGS)
        .unwrap()
        .insert("alpha", "changed")
        .unwrap();
    writer.commit().unwrap();
    assert_conflict(reader.commit());
    let read = db.begin_read().unwrap();
    assert!(
        read.open_table(STRINGS)
            .unwrap()
            .get("Delta")
            .unwrap()
            .is_none()
    );
}

#[test]
fn custom_comparator_preserves_or_replaces_key_encoding_like_a_classic_writer() {
    const CLASSIC: TableDefinition<FoldedReverseKey, &str> = TableDefinition::new("classic");
    const CONCURRENT: TableDefinition<FoldedReverseKey, &str> = TableDefinition::new("concurrent");
    let file = create_tempfile();
    {
        let db = Database::create(file.path()).unwrap();
        let initial = db.begin_write().unwrap();
        for definition in [CLASSIC, CONCURRENT] {
            initial
                .open_table(definition)
                .unwrap()
                .insert("alpha", "initial")
                .unwrap();
        }
        initial.commit().unwrap();
        for replace_key in [false, true] {
            let spelling = if replace_key { "AlPhA" } else { "aLpHa" };
            let expected = if replace_key { "AlPhA" } else { "alpha" };
            let mut txn = db.begin_concurrent_write().unwrap();
            {
                let mut table = txn.open_table(CONCURRENT).unwrap();
                assert!(table.get("ALPHA").unwrap().is_some());
                if replace_key {
                    table.remove("ALPHA").unwrap();
                }
                table.insert(spelling, "changed").unwrap();
                let (key, value) = table.iter().unwrap().next().unwrap().unwrap();
                assert_eq!(key.value(), expected);
                assert_eq!(value.value(), "changed");
            }
            let classic = db.begin_write().unwrap();
            {
                let mut table = classic.open_table(CLASSIC).unwrap();
                assert!(table.get("ALPHA").unwrap().is_some());
                if replace_key {
                    table.remove("ALPHA").unwrap();
                }
                table.insert(spelling, "changed").unwrap();
            }
            classic.commit().unwrap();
            txn.commit().unwrap();
            let read = db.begin_read().unwrap();
            for definition in [CLASSIC, CONCURRENT] {
                let table = read.open_table(definition).unwrap();
                let (key, value) = table.iter().unwrap().next().unwrap().unwrap();
                assert_eq!(key.value(), expected);
                assert_eq!(value.value(), "changed");
            }
        }
    }
    let db = Database::open(file.path()).unwrap();
    let read = db.begin_read().unwrap();
    for definition in [CLASSIC, CONCURRENT] {
        let table = read.open_table(definition).unwrap();
        let (key, value) = table.iter().unwrap().next().unwrap().unwrap();
        assert_eq!(key.value(), "AlPhA");
        assert_eq!(value.value(), "changed");
    }
}

#[test]
fn unrelated_classic_writes_are_preserved_and_changed_dependencies_conflict() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut txn = db.begin_concurrent_write().unwrap();
    txn.open_table(FIRST).unwrap().insert(1, 11).unwrap();
    let classic = db.begin_write().unwrap();
    classic.open_table(FIRST).unwrap().insert(2, 22).unwrap();
    classic.commit().unwrap();
    txn.commit().unwrap();
    assert_eq!(get(&db, FIRST, 1), Some(11));
    assert_eq!(get(&db, FIRST, 2), Some(22));

    let mut txn = db.begin_concurrent_write().unwrap();
    {
        let mut table = txn.open_table(FIRST).unwrap();
        assert_eq!(table.get(1).unwrap().unwrap().value(), 11);
        table.insert(3, 33).unwrap();
    }
    let classic = db.begin_write().unwrap();
    classic.open_table(FIRST).unwrap().insert(1, 111).unwrap();
    classic.commit().unwrap();
    assert_conflict(txn.commit());
    assert_eq!(get(&db, FIRST, 3), None);
}

#[test]
fn a_conflict_in_a_later_table_does_not_publish_earlier_writes() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut txn = db.begin_concurrent_write().unwrap();
    txn.open_table(FIRST).unwrap().insert(100, 1000).unwrap();
    txn.open_table(SECOND).unwrap().insert(1, 111).unwrap();
    let classic = db.begin_write().unwrap();
    classic.open_table(SECOND).unwrap().insert(1, 222).unwrap();
    classic.commit().unwrap();
    assert_conflict(txn.commit());
    assert_eq!(get(&db, FIRST, 100), None);
    assert_eq!(get(&db, SECOND, 1), Some(222));
}

#[cfg(panic = "unwind")]
#[test]
fn every_table_is_validated_before_any_staged_value_is_replayed() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    static REPLAY_FORBIDDEN: AtomicBool = AtomicBool::new(false);

    #[derive(Debug)]
    struct ReplayValue;

    impl Value for ReplayValue {
        type SelfType<'a> = u64;
        type AsBytes<'a> = [u8; 8];

        fn fixed_width() -> Option<usize> {
            Some(8)
        }

        fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
        where
            Self: 'a,
        {
            u64::from_le_bytes(data.try_into().unwrap())
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
            assert!(
                *value != 7000 || !REPLAY_FORBIDDEN.load(AtomicOrdering::Relaxed),
                "replayed before validating a conflicting later table"
            );
            value.to_le_bytes()
        }

        fn type_name() -> TypeName {
            TypeName::new("concurrent_writes::ReplayValue")
        }
    }

    const EARLY: TableDefinition<u64, ReplayValue> = TableDefinition::new("a_replay");
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let classic = db.begin_write().unwrap();
    classic.open_table(EARLY).unwrap();
    classic.commit().unwrap();
    let mut txn = db.begin_concurrent_write().unwrap();
    txn.open_table(EARLY).unwrap().insert(7, 7000).unwrap();
    txn.open_table(SECOND).unwrap().insert(1, 111).unwrap();
    let classic = db.begin_write().unwrap();
    classic.open_table(SECOND).unwrap().insert(1, 222).unwrap();
    classic.commit().unwrap();

    REPLAY_FORBIDDEN.store(true, AtomicOrdering::Relaxed);
    let result = catch_unwind(AssertUnwindSafe(|| txn.commit()));
    REPLAY_FORBIDDEN.store(false, AtomicOrdering::Relaxed);
    assert_conflict(result.expect("staged data was replayed before validation completed"));
    assert!(
        db.begin_read()
            .unwrap()
            .open_table(EARLY)
            .unwrap()
            .get(7)
            .unwrap()
            .is_none()
    );
}

#[test]
fn opening_an_empty_table_creates_it_only_on_commit() {
    const EMPTY: TableDefinition<u64, u64> = TableDefinition::new("empty");
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let mut txn = db.begin_concurrent_write().unwrap();
    assert!(txn.open_table(EMPTY).unwrap().is_empty().unwrap());
    assert!(db.begin_read().unwrap().open_table(EMPTY).is_err());
    txn.commit().unwrap();
    assert_eq!(
        db.begin_read()
            .unwrap()
            .open_table(EMPTY)
            .unwrap()
            .len()
            .unwrap(),
        0
    );
}

#[test]
fn schema_replacement_conflicts_with_pending_creation() {
    const NEW: TableDefinition<u64, u64> = TableDefinition::new("new");
    const WRONG: TableDefinition<&str, &str> = TableDefinition::new("new");
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let mut txn = db.begin_concurrent_write().unwrap();
    txn.open_table(NEW).unwrap().insert(1, 10).unwrap();
    let classic = db.begin_write().unwrap();
    classic
        .open_table(WRONG)
        .unwrap()
        .insert("key", "value")
        .unwrap();
    classic.commit().unwrap();
    assert_conflict(txn.commit());
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(WRONG)
            .unwrap()
            .get("key")
            .unwrap()
            .unwrap()
            .value(),
        "value"
    );
}

#[test]
fn dropping_and_recreating_a_table_conflicts_with_its_old_contents() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    populate(&db);
    let mut txn = db.begin_concurrent_write().unwrap();
    txn.open_table(FIRST).unwrap().insert(1, 111).unwrap();
    let classic = db.begin_write().unwrap();
    assert!(classic.delete_table(FIRST).unwrap());
    classic.open_table(FIRST).unwrap().insert(1, 222).unwrap();
    classic.commit().unwrap();
    assert_conflict(txn.commit());
    assert_eq!(get(&db, FIRST, 1), Some(222));
    assert_eq!(get(&db, FIRST, 2), None);
}

#[test]
fn durable_commits_reopen_and_leave_existing_snapshots_unchanged() {
    let file = create_tempfile();
    {
        let db = Database::create(file.path()).unwrap();
        populate(&db);
        let snapshot = db.begin_read().unwrap();
        let mut first = db.begin_concurrent_write().unwrap();
        let mut second = db.begin_concurrent_write().unwrap();
        first.open_table(FIRST).unwrap().insert(1, 111).unwrap();
        second.open_table(SECOND).unwrap().insert(2, 222).unwrap();
        first.commit().unwrap();
        second.commit().unwrap();
        assert_eq!(
            snapshot
                .open_table(FIRST)
                .unwrap()
                .get(1)
                .unwrap()
                .unwrap()
                .value(),
            10
        );
        assert_eq!(
            snapshot
                .open_table(SECOND)
                .unwrap()
                .get(2)
                .unwrap()
                .unwrap()
                .value(),
            20
        );
        assert_eq!(get(&db, FIRST, 1), Some(111));
        assert_eq!(get(&db, SECOND, 2), Some(222));
    }
    let mut db = Database::open(file.path()).unwrap();
    assert_eq!(get(&db, FIRST, 1), Some(111));
    assert_eq!(get(&db, SECOND, 2), Some(222));
    assert!(db.check_integrity().unwrap());
}

#[derive(Clone, Debug, Default)]
struct ObservedBackend {
    bytes: Arc<Mutex<Vec<u8>>>,
    writes: Arc<AtomicUsize>,
    resizes: Arc<AtomicUsize>,
    fail_sync: Arc<AtomicBool>,
}

impl StorageBackend for ObservedBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        Ok(self.bytes.lock().unwrap().len().try_into().unwrap())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        let offset = usize::try_from(offset).unwrap();
        let bytes = self.bytes.lock().unwrap();
        let source = bytes
            .get(offset..offset + out.len())
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        out.copy_from_slice(source);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.resizes.fetch_add(1, AtomicOrdering::Relaxed);
        self.bytes
            .lock()
            .unwrap()
            .resize(len.try_into().unwrap(), 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        if self.fail_sync.load(AtomicOrdering::Relaxed) {
            Err(std::io::Error::other("injected sync failure"))
        } else {
            Ok(())
        }
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.writes.fetch_add(1, AtomicOrdering::Relaxed);
        let offset = usize::try_from(offset).unwrap();
        let mut bytes = self.bytes.lock().unwrap();
        let target = bytes
            .get_mut(offset..offset + data.len())
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        target.copy_from_slice(data);
        Ok(())
    }
}

#[test]
fn preparation_abort_and_drop_do_not_write_or_resize_storage() {
    const NEW: TableDefinition<u64, u64> = TableDefinition::new("new");
    let backend = ObservedBackend::default();
    let db = Database::builder()
        .create_with_backend(backend.clone())
        .unwrap();
    populate(&db);
    let original = backend.bytes.lock().unwrap().clone();
    let writes = backend.writes.load(AtomicOrdering::Relaxed);
    let resizes = backend.resizes.load(AtomicOrdering::Relaxed);
    for abort in [true, false] {
        let mut txn = db.begin_concurrent_write().unwrap();
        txn.open_table(FIRST).unwrap().insert(1, 111).unwrap();
        txn.open_table(NEW).unwrap().insert(100, 1000).unwrap();
        if abort {
            txn.abort();
        }
    }
    assert_eq!(*backend.bytes.lock().unwrap(), original);
    assert_eq!(backend.writes.load(AtomicOrdering::Relaxed), writes);
    assert_eq!(backend.resizes.load(AtomicOrdering::Relaxed), resizes);
    assert_eq!(get(&db, FIRST, 1), Some(10));
    assert!(db.begin_read().unwrap().open_table(NEW).is_err());
}

#[test]
fn failed_commit_does_not_leave_a_partially_applied_transaction() {
    let backend = ObservedBackend::default();
    {
        let db = Database::builder()
            .create_with_backend(backend.clone())
            .unwrap();
        populate(&db);
        let mut txn = db.begin_concurrent_write().unwrap();
        txn.open_table(FIRST).unwrap().insert(1, 111).unwrap();
        txn.open_table(SECOND).unwrap().insert(2, 222).unwrap();
        backend.fail_sync.store(true, AtomicOrdering::Relaxed);
        assert!(txn.commit().is_err());
    }
    backend.fail_sync.store(false, AtomicOrdering::Relaxed);
    let mut db = Database::builder().create_with_backend(backend).unwrap();
    let observed = (get(&db, FIRST, 1), get(&db, SECOND, 2));
    assert!(matches!(
        observed,
        (Some(10), Some(20)) | (Some(111), Some(222))
    ));
    assert!(db.check_integrity().unwrap());
}

#[test]
fn an_old_writer_merges_after_many_disjoint_optimistic_commits() {
    let mut db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    populate(&db);
    let mut old = db.begin_concurrent_write().unwrap();
    old.open_table(FIRST).unwrap().insert(1, 111).unwrap();
    for key in 100..116 {
        let mut txn = db.begin_concurrent_write().unwrap();
        txn.open_table(FIRST)
            .unwrap()
            .insert(key, key * 10)
            .unwrap();
        txn.commit().unwrap();
    }
    old.commit().unwrap();
    assert_eq!(get(&db, FIRST, 1), Some(111));
    for key in 100..116 {
        assert_eq!(get(&db, FIRST, key), Some(key * 10));
    }
    assert!(db.check_integrity().unwrap());
}

#[test]
fn an_overlapping_commit_is_detected_among_disjoint_optimistic_commits() {
    let db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    populate(&db);
    let mut old = db.begin_concurrent_write().unwrap();
    old.open_table(FIRST).unwrap().insert(1, 111).unwrap();
    for key in [100, 101, 1, 102, 103] {
        let mut txn = db.begin_concurrent_write().unwrap();
        txn.open_table(FIRST)
            .unwrap()
            .insert(key, key * 100)
            .unwrap();
        txn.commit().unwrap();
    }
    assert_conflict(old.commit());
    assert_eq!(get(&db, FIRST, 1), Some(100));
}

#[test]
fn a_classic_commit_gap_revalidates_preimages_instead_of_trusting_partial_history() {
    for changed_dependency in [false, true] {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        populate(&db);
        let mut old = db.begin_concurrent_write().unwrap();
        old.open_table(FIRST).unwrap().insert(1, 111).unwrap();
        let mut before_gap = db.begin_concurrent_write().unwrap();
        before_gap
            .open_table(FIRST)
            .unwrap()
            .insert(100, 1000)
            .unwrap();
        before_gap.commit().unwrap();
        let classic = db.begin_write().unwrap();
        classic
            .open_table(FIRST)
            .unwrap()
            .insert(if changed_dependency { 1 } else { 500 }, 5000)
            .unwrap();
        classic.commit().unwrap();
        let mut after_gap = db.begin_concurrent_write().unwrap();
        after_gap
            .open_table(FIRST)
            .unwrap()
            .insert(101, 1010)
            .unwrap();
        after_gap.commit().unwrap();
        if changed_dependency {
            assert_conflict(old.commit());
            assert_eq!(get(&db, FIRST, 1), Some(5000));
        } else {
            old.commit().unwrap();
            assert_eq!(get(&db, FIRST, 1), Some(111));
            assert_eq!(get(&db, FIRST, 500), Some(5000));
        }
    }
}

#[test]
fn table_wide_reads_are_not_weakened_by_disjoint_point_write_history() {
    for query in 0..3 {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        populate(&db);
        let mut old = db.begin_concurrent_write().unwrap();
        {
            let mut table = old.open_table(FIRST).unwrap();
            match query {
                0 => assert_eq!(table.len().unwrap(), 2),
                1 => assert!(!table.is_empty().unwrap()),
                2 => assert!(table.range(10..20).unwrap().next().is_none()),
                _ => unreachable!(),
            }
        }
        old.open_table(SECOND).unwrap().insert(100, 1000).unwrap();
        let mut writer = db.begin_concurrent_write().unwrap();
        writer.open_table(FIRST).unwrap().insert(15, 150).unwrap();
        writer.commit().unwrap();
        assert_conflict(old.commit());
        assert_eq!(get(&db, SECOND, 100), None);
    }
}

#[test]
fn optimistic_history_intersections_use_the_custom_key_comparator() {
    const STRINGS: TableDefinition<FoldedReverseKey, &str> = TableDefinition::new("strings");
    let db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    let initial = db.begin_write().unwrap();
    initial
        .open_table(STRINGS)
        .unwrap()
        .insert("alpha", "initial")
        .unwrap();
    initial.commit().unwrap();
    let mut old = db.begin_concurrent_write().unwrap();
    {
        let mut table = old.open_table(STRINGS).unwrap();
        assert_eq!(table.get("ALPHA").unwrap().unwrap().value(), "initial");
        table.insert("Delta", "not committed").unwrap();
    }
    for key in ["Beta", "alpha", "Gamma"] {
        let mut txn = db.begin_concurrent_write().unwrap();
        txn.open_table(STRINGS)
            .unwrap()
            .insert(key, "changed")
            .unwrap();
        txn.commit().unwrap();
    }
    assert_conflict(old.commit());
    let read = db.begin_read().unwrap();
    let table = read.open_table(STRINGS).unwrap();
    assert_eq!(table.get("ALPHA").unwrap().unwrap().value(), "changed");
    assert!(table.get("Delta").unwrap().is_none());
}

#[test]
fn history_eviction_revalidates_writers_older_than_the_retained_commits() {
    let mut db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    populate(&db);
    let mut disjoint = db.begin_concurrent_write().unwrap();
    disjoint.open_table(FIRST).unwrap().insert(2, 222).unwrap();
    let mut overlapping = db.begin_concurrent_write().unwrap();
    overlapping
        .open_table(FIRST)
        .unwrap()
        .insert(1, 111)
        .unwrap();
    let mut earliest = db.begin_concurrent_write().unwrap();
    earliest.open_table(FIRST).unwrap().insert(1, 999).unwrap();
    earliest.commit().unwrap();
    // The bounded history retains at most 64 commits. Its record of the changed
    // dependency is evicted while both original snapshots remain pinned.
    for key in 100..180 {
        let mut txn = db.begin_concurrent_write().unwrap();
        txn.open_table(FIRST)
            .unwrap()
            .insert(key, key * 10)
            .unwrap();
        txn.commit().unwrap();
    }
    disjoint.commit().unwrap();
    assert_conflict(overlapping.commit());
    assert_eq!(get(&db, FIRST, 1), Some(999));
    assert_eq!(get(&db, FIRST, 2), Some(222));
    assert_eq!(get(&db, FIRST, 179), Some(1790));
    assert!(db.check_integrity().unwrap());
}

#[test]
fn a_commit_larger_than_the_history_budget_keeps_dependency_validation_correct() {
    const BYTES: TableDefinition<&[u8], u64> = TableDefinition::new("bytes");
    let db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    let initial = db.begin_write().unwrap();
    {
        let mut table = initial.open_table(BYTES).unwrap();
        table.insert(b"watched".as_slice(), 1).unwrap();
        table.insert(b"unchanged".as_slice(), 7).unwrap();
    }
    initial.commit().unwrap();
    let mut overlapping = db.begin_concurrent_write().unwrap();
    {
        let mut table = overlapping.open_table(BYTES).unwrap();
        assert_eq!(
            table.get(b"watched".as_slice()).unwrap().unwrap().value(),
            1
        );
        table.insert(b"not applied".as_slice(), 99).unwrap();
    }
    let mut disjoint = db.begin_concurrent_write().unwrap();
    {
        let mut table = disjoint.open_table(BYTES).unwrap();
        assert_eq!(
            table.get(b"unchanged".as_slice()).unwrap().unwrap().value(),
            7
        );
        table.insert(b"safe".as_slice(), 99).unwrap();
    }
    let oversized_key = vec![0xFF; 70 * 1024];
    let mut large = db.begin_concurrent_write().unwrap();
    {
        let mut table = large.open_table(BYTES).unwrap();
        table.insert(b"watched".as_slice(), 2).unwrap();
        table.insert(oversized_key.as_slice(), 3).unwrap();
    }
    large.commit().unwrap();
    let mut later = db.begin_concurrent_write().unwrap();
    later
        .open_table(BYTES)
        .unwrap()
        .insert(b"later".as_slice(), 4)
        .unwrap();
    later.commit().unwrap();
    disjoint.commit().unwrap();
    assert_conflict(overlapping.commit());
    let read = db.begin_read().unwrap();
    let table = read.open_table(BYTES).unwrap();
    assert_eq!(
        table.get(b"watched".as_slice()).unwrap().unwrap().value(),
        2
    );
    assert_eq!(table.get(b"safe".as_slice()).unwrap().unwrap().value(), 99);
    assert_eq!(
        table
            .get(oversized_key.as_slice())
            .unwrap()
            .unwrap()
            .value(),
        3
    );
    assert!(table.get(b"not applied".as_slice()).unwrap().is_none());
}
