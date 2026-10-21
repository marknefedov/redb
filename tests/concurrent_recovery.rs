use redb::ReadableTable;
use redb::{ConcurrentWriteError, Database, Durability, ReadableDatabase, TableDefinition};

const DATA: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
const MARKERS: TableDefinition<u64, u64> = TableDefinition::new("markers");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn payload(key: u64, generation: u64) -> Vec<u8> {
    let mut bytes = vec![generation as u8; 2048];
    bytes[..8].copy_from_slice(&key.to_le_bytes());
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    bytes
}

fn marker(database: &Database, key: u64) -> Option<u64> {
    database
        .begin_read()
        .unwrap()
        .open_table(MARKERS)
        .unwrap()
        .get(key)
        .unwrap()
        .map(|value| value.value())
}

#[test]
fn failed_schema_and_table_kind_reads_are_serializable_dependencies() {
    use redb::{MultimapTableDefinition, TableError};

    const REQUESTED: TableDefinition<u64, u64> = TableDefinition::new("schema");
    const WRONG_SCHEMA: TableDefinition<&str, &str> = TableDefinition::new("schema");
    const WRONG_KIND: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("schema");
    for multimap in [false, true] {
        let file = create_tempfile();
        let database = Database::create(file.path()).unwrap();
        let initial = database.begin_write().unwrap();
        initial.open_table(MARKERS).unwrap().insert(0, 0).unwrap();
        if multimap {
            initial.open_multimap_table(WRONG_KIND).unwrap();
        } else {
            initial.open_table(WRONG_SCHEMA).unwrap();
        }
        initial.commit().unwrap();

        let mut optimistic = database.begin_concurrent_write().unwrap();
        optimistic
            .open_table(MARKERS)
            .unwrap()
            .insert(0, 1)
            .unwrap();
        let failure = optimistic.open_table(REQUESTED).err().unwrap();
        if multimap {
            assert!(matches!(failure, TableError::TableIsMultimap(_)));
        } else {
            assert!(matches!(failure, TableError::TableTypeMismatch { .. }));
        }

        let classic = database.begin_write().unwrap();
        assert_eq!(
            classic
                .open_table(MARKERS)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            0
        );
        if multimap {
            assert!(classic.delete_multimap_table(WRONG_KIND).unwrap());
        } else {
            assert!(classic.delete_table(WRONG_SCHEMA).unwrap());
        }
        classic
            .open_table(REQUESTED)
            .unwrap()
            .insert(1, 100)
            .unwrap();
        classic.commit().unwrap();

        // Neither serial order can explain both the schema error and the classic
        // writer's observation of the old marker, so the optimistic write must fail.
        assert!(matches!(
            optimistic.commit(),
            Err(ConcurrentWriteError::Conflict { .. })
        ));
        assert_eq!(marker(&database, 0), Some(0));
    }
}

#[test]
fn handled_schema_errors_can_commit_if_the_failed_target_did_not_change() {
    use redb::TableError;

    const REQUESTED: TableDefinition<u64, u64> = TableDefinition::new("schema");
    const WRONG_SCHEMA: TableDefinition<&str, &str> = TableDefinition::new("schema");
    const PRIVATE: TableDefinition<u64, u64> = TableDefinition::new("private");
    const WRONG_PRIVATE: TableDefinition<&str, &str> = TableDefinition::new("private");
    let file = create_tempfile();
    let database = Database::create(file.path()).unwrap();
    let initial = database.begin_write().unwrap();
    initial.open_table(MARKERS).unwrap();
    initial.open_table(WRONG_SCHEMA).unwrap();
    initial.commit().unwrap();

    let mut optimistic = database.begin_concurrent_write().unwrap();
    optimistic
        .open_table(MARKERS)
        .unwrap()
        .insert(1, 11)
        .unwrap();
    assert!(matches!(
        optimistic.open_table(REQUESTED),
        Err(TableError::TableTypeMismatch { .. })
    ));
    optimistic
        .open_table(PRIVATE)
        .unwrap()
        .insert(1, 100)
        .unwrap();
    assert!(matches!(
        optimistic.open_table(WRONG_PRIVATE),
        Err(TableError::TableTypeMismatch { .. })
    ));
    optimistic.commit().unwrap();
    assert_eq!(marker(&database, 1), Some(11));
    assert_eq!(
        database
            .begin_read()
            .unwrap()
            .open_table(PRIVATE)
            .unwrap()
            .get(1)
            .unwrap()
            .unwrap()
            .value(),
        100
    );
}

#[test]
fn pinned_optimistic_snapshots_survive_mixed_non_durable_writers_and_flush() {
    let file = create_tempfile();
    let mut expected: Vec<_> = (0..32).map(|key| payload(key, 0)).collect();
    {
        let database = Database::create(file.path()).unwrap();
        let initial = database.begin_write().unwrap();
        {
            let mut data = initial.open_table(DATA).unwrap();
            for (key, bytes) in expected.iter().enumerate() {
                data.insert(key as u64, bytes.as_slice()).unwrap();
            }
        }
        initial.open_table(MARKERS).unwrap();
        initial.commit().unwrap();

        let durable_snapshot = database.begin_read().unwrap();
        let mut disjoint = database.begin_concurrent_write().unwrap();
        {
            let mut data = disjoint.open_table(DATA).unwrap();
            assert_eq!(data.get(31).unwrap().unwrap().value(), payload(31, 0));
        }
        disjoint.open_table(MARKERS).unwrap().insert(1, 11).unwrap();
        disjoint.set_durability(Durability::None).unwrap();
        let mut non_durable_snapshot = None;

        for round in 0..16u64 {
            let key = round % 8;
            let bytes = payload(key, round + 1);
            let mut classic = database.begin_write().unwrap();
            classic.set_durability(Durability::None).unwrap();
            classic
                .open_table(DATA)
                .unwrap()
                .insert(key, bytes.as_slice())
                .unwrap();
            classic.commit().unwrap();
            expected[key as usize] = bytes;
            if round == 0 {
                non_durable_snapshot = Some(database.begin_concurrent_write().unwrap());
            }

            let key = key + 8;
            let bytes = payload(key, round + 1);
            let mut optimistic = database.begin_concurrent_write().unwrap();
            optimistic.set_durability(Durability::None).unwrap();
            optimistic
                .open_table(DATA)
                .unwrap()
                .insert(key, bytes.as_slice())
                .unwrap();
            optimistic.commit().unwrap();
            expected[key as usize] = bytes;
        }

        let mut non_durable_snapshot = non_durable_snapshot.unwrap();
        {
            let mut data = non_durable_snapshot.open_table(DATA).unwrap();
            assert_eq!(data.get(0).unwrap().unwrap().value(), payload(0, 1));
            assert_eq!(data.get(1).unwrap().unwrap().value(), payload(1, 0));
        }
        non_durable_snapshot
            .open_table(MARKERS)
            .unwrap()
            .insert(100, 100)
            .unwrap();
        assert!(matches!(
            non_durable_snapshot.commit(),
            Err(ConcurrentWriteError::Conflict { .. })
        ));
        assert_eq!(marker(&database, 100), None);
        disjoint.commit().unwrap();

        let mut durable = database.begin_concurrent_write().unwrap();
        durable.open_table(MARKERS).unwrap().insert(2, 22).unwrap();
        durable.commit().unwrap();
        let durable = database.begin_write().unwrap();
        durable.open_table(MARKERS).unwrap().insert(3, 33).unwrap();
        durable.commit().unwrap();

        let original = durable_snapshot.open_table(DATA).unwrap();
        for key in 0..32 {
            assert_eq!(original.get(key).unwrap().unwrap().value(), payload(key, 0));
        }
    }

    let mut database = Database::open(file.path()).unwrap();
    {
        let read = database.begin_read().unwrap();
        let data = read.open_table(DATA).unwrap();
        for (key, bytes) in expected.iter().enumerate() {
            assert_eq!(data.get(key as u64).unwrap().unwrap().value(), bytes);
        }
    }
    for (key, value) in [(1, 11), (2, 22), (3, 33)] {
        assert_eq!(marker(&database, key), Some(value));
    }
    assert_eq!(marker(&database, 100), None);
    assert!(database.check_integrity().unwrap());
}

#[cfg(panic = "unwind")]
#[test]
fn a_codec_panic_during_replay_does_not_publish_earlier_table_changes() {
    use redb::{TypeName, Value};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, Ordering};

    static REPLAY: AtomicBool = AtomicBool::new(false);
    static EARLY_REPLAYED: AtomicBool = AtomicBool::new(false);

    #[derive(Debug)]
    struct ReplayValue;

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
            if REPLAY.load(Ordering::Relaxed) {
                if *value == 111 {
                    EARLY_REPLAYED.store(true, Ordering::Relaxed);
                }
                assert_ne!(*value, 9001, "injected codec panic during replay");
            }
            value.to_le_bytes()
        }

        fn type_name() -> TypeName {
            TypeName::new("concurrent_recovery::ReplayValue")
        }
    }

    const EARLY: TableDefinition<u64, ReplayValue> = TableDefinition::new("a_first");
    const LATE: TableDefinition<u64, ReplayValue> = TableDefinition::new("z_last");
    let file = create_tempfile();
    {
        let database = Database::create(file.path()).unwrap();
        let initial = database.begin_write().unwrap();
        initial.open_table(EARLY).unwrap().insert(1, 10).unwrap();
        initial.open_table(LATE).unwrap();
        initial.commit().unwrap();

        let mut transaction = database.begin_concurrent_write().unwrap();
        transaction
            .open_table(EARLY)
            .unwrap()
            .insert(1, 111)
            .unwrap();
        transaction
            .open_table(LATE)
            .unwrap()
            .insert(7, 9001)
            .unwrap();
        REPLAY.store(true, Ordering::Relaxed);
        let result = catch_unwind(AssertUnwindSafe(|| transaction.commit()));
        REPLAY.store(false, Ordering::Relaxed);
        assert!(result.is_err());
        assert!(EARLY_REPLAYED.load(Ordering::Relaxed));
        {
            let read = database.begin_read().unwrap();
            assert_eq!(
                read.open_table(EARLY)
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .unwrap()
                    .value(),
                10
            );
            assert!(read.open_table(LATE).unwrap().get(7).unwrap().is_none());
        }

        let later = database.begin_write().unwrap();
        later.open_table(EARLY).unwrap().insert(2, 222).unwrap();
        later.commit().unwrap();
        let mut later = database.begin_concurrent_write().unwrap();
        later.open_table(EARLY).unwrap().insert(3, 333).unwrap();
        later.commit().unwrap();
    }

    let mut database = Database::open(file.path()).unwrap();
    {
        let read = database.begin_read().unwrap();
        let early = read.open_table(EARLY).unwrap();
        assert_eq!(early.get(1).unwrap().unwrap().value(), 10);
        assert_eq!(early.get(2).unwrap().unwrap().value(), 222);
        assert_eq!(early.get(3).unwrap().unwrap().value(), 333);
        assert!(read.open_table(LATE).unwrap().get(7).unwrap().is_none());
    }
    assert!(database.check_integrity().unwrap());
}

#[cfg(all(
    feature = "experimental-multiprocess",
    any(target_os = "linux", target_vendor = "apple", windows)
))]
#[test]
fn optimistic_commit_validates_under_a_multi_writer_lock() {
    use redb::{ConcurrencyMode, SetDurabilityError};
    use std::sync::mpsc;
    use std::time::Duration;

    let (complete, completed) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let file = create_tempfile();
        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriter);
        let database = builder.create(file.path()).unwrap();
        let initial = database.begin_write().unwrap();
        initial.open_table(MARKERS).unwrap().insert(1, 10).unwrap();
        initial.commit().unwrap();
        let peer = builder.open(file.path()).unwrap();

        let mut optimistic = database.begin_concurrent_write().unwrap();
        {
            let mut markers = optimistic.open_table(MARKERS).unwrap();
            assert_eq!(markers.get(1).unwrap().unwrap().value(), 10);
            markers.insert(3, 30).unwrap();
        }
        let classic = peer.begin_write().unwrap();
        classic.open_table(MARKERS).unwrap().insert(2, 20).unwrap();
        classic.commit().unwrap();
        optimistic.commit().unwrap();
        assert_eq!(marker(&database, 2), Some(20));
        assert_eq!(marker(&peer, 3), Some(30));

        let mut optimistic = database.begin_concurrent_write().unwrap();
        {
            let mut markers = optimistic.open_table(MARKERS).unwrap();
            assert_eq!(markers.get(1).unwrap().unwrap().value(), 10);
            markers.insert(4, 40).unwrap();
        }
        let classic = peer.begin_write().unwrap();
        classic.open_table(MARKERS).unwrap().insert(1, 11).unwrap();
        classic.commit().unwrap();
        assert!(matches!(
            optimistic.commit(),
            Err(ConcurrentWriteError::Conflict { .. })
        ));
        assert_eq!(marker(&database, 4), None);
        let mut optimistic = database.begin_concurrent_write().unwrap();
        assert!(matches!(
            optimistic.set_durability(Durability::None),
            Err(SetDurabilityError::NonDurableCommitUnsupported)
        ));
        optimistic.abort();
        complete.send(()).unwrap();
    });
    completed
        .recv_timeout(Duration::from_secs(30))
        .expect("optimistic commit blocked while already holding the writer lock");
    worker.join().unwrap();
}
