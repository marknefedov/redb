use redb::backends::FileBackend;
use redb::{CommitMetrics, Database, Durability, StorageBackend, TableDefinition};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const DATA: TableDefinition<u64, &[u8]> = TableDefinition::new("data");

#[derive(Debug, Default)]
struct BackendCounters {
    write_calls: AtomicU64,
    written_bytes: AtomicU64,
    sync_calls: AtomicU64,
    fail_write: AtomicBool,
    fail_sync: AtomicBool,
    panic_sync: AtomicBool,
}

impl BackendCounters {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.write_calls.load(Ordering::Relaxed),
            self.written_bytes.load(Ordering::Relaxed),
            self.sync_calls.load(Ordering::Relaxed),
        )
    }
}

#[derive(Debug)]
struct MeasuredBackend {
    file: FileBackend,
    counters: Arc<BackendCounters>,
}

impl StorageBackend for MeasuredBackend {
    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.counters.sync_calls.fetch_add(1, Ordering::Relaxed);
        assert!(
            !self.counters.panic_sync.swap(false, Ordering::Relaxed),
            "injected synchronization panic"
        );
        if self.counters.fail_sync.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected synchronization failure"));
        }
        self.file.sync_data()
    }

    fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.counters.write_calls.fetch_add(1, Ordering::Relaxed);
        if self.counters.fail_write.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected write failure"));
        }
        self.file.write(offset, bytes)?;
        self.counters
            .written_bytes
            .fetch_add(u64::try_from(bytes.len()).unwrap(), Ordering::Relaxed);
        Ok(())
    }

    fn close(&self) -> io::Result<()> {
        self.file.close()
    }
}

fn database() -> (Database, Arc<BackendCounters>, tempfile::NamedTempFile) {
    let file = if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    };
    let counters = Arc::new(BackendCounters::default());
    let database = Database::builder()
        .create_with_backend(MeasuredBackend {
            file: FileBackend::new(file.reopen().unwrap()).unwrap(),
            counters: counters.clone(),
        })
        .unwrap();
    let initial = database.begin_write().unwrap();
    initial
        .open_table(DATA)
        .unwrap()
        .insert(0, &[0; 64][..])
        .unwrap();
    initial.commit().unwrap();
    (database, counters, file)
}

fn assert_backend_counts(
    metrics: &CommitMetrics,
    counters: &BackendCounters,
    baseline: (u64, u64, u64),
) {
    let current = counters.snapshot();
    assert_eq!(metrics.write.calls, current.0 - baseline.0);
    assert_eq!(metrics.written_bytes, current.1 - baseline.1);
    assert_eq!(metrics.sync.calls, current.2 - baseline.2);
}

#[test]
fn collection_is_optional_and_matches_native_backend_operations() {
    let (database, counters, _file) = database();
    assert!(counters.write_calls.load(Ordering::Relaxed) > 0);
    assert!(counters.sync_calls.load(Ordering::Relaxed) > 0);
    assert_eq!(database.commit_metrics(), CommitMetrics::default());

    let baseline = counters.snapshot();
    database.set_commit_metrics_enabled(true);
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(DATA)
        .unwrap()
        .insert(1, &[1; 64][..])
        .unwrap();
    transaction.commit().unwrap();
    let durable = database.commit_metrics();
    assert_eq!(durable.commit.calls, 1);
    assert_eq!(durable.physical_commits, 1);
    assert_eq!(durable.durable_commits, 1);
    assert_eq!(durable.commit_errors, 0);
    assert_eq!(durable.write_errors, 0);
    assert_eq!(durable.sync_errors, 0);
    assert!(durable.write.calls > 0);
    assert!(durable.sync.calls > 0);
    assert_backend_counts(&durable, &counters, baseline);

    let mut transaction = database.begin_write().unwrap();
    transaction.set_durability(Durability::None).unwrap();
    transaction
        .open_table(DATA)
        .unwrap()
        .insert(2, &[2; 64][..])
        .unwrap();
    transaction.commit().unwrap();
    let non_durable = database.commit_metrics();
    assert_eq!(non_durable.commit.calls, 2);
    assert_eq!(non_durable.physical_commits, 2);
    assert_eq!(non_durable.durable_commits, 1);
    assert_backend_counts(&non_durable, &counters, baseline);

    database.set_commit_metrics_enabled(false);
    let before_disabled = database.commit_metrics();
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(DATA)
        .unwrap()
        .insert(3, &[3; 64][..])
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(database.commit_metrics(), before_disabled);
}

#[test]
fn errors_and_panics_are_measured_without_claiming_successful_bytes_or_commits() {
    let failures = if cfg!(panic = "unwind") { 3 } else { 2 };
    for failure in 0..failures {
        let (database, counters, _file) = database();
        let baseline = counters.snapshot();
        database.set_commit_metrics_enabled(true);
        let transaction = database.begin_write().unwrap();
        transaction
            .open_table(DATA)
            .unwrap()
            .insert(1, &[1; 64][..])
            .unwrap();
        match failure {
            0 => counters.fail_write.store(true, Ordering::Relaxed),
            1 => counters.fail_sync.store(true, Ordering::Relaxed),
            _ => counters.panic_sync.store(true, Ordering::Relaxed),
        }
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| transaction.commit()));
        if failure == 2 {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        let metrics = database.commit_metrics();
        assert_eq!(metrics.commit.calls, 1);
        assert_eq!(metrics.physical_commits, 0);
        assert_eq!(metrics.durable_commits, 0);
        assert_eq!(metrics.commit_errors, 1);
        assert_eq!(metrics.write_errors, u64::from(failure == 0));
        assert_eq!(metrics.sync_errors, u64::from(failure != 0));
        assert_backend_counts(&metrics, &counters, baseline);
    }
}
