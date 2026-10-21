//! Compare serialized writers with optimistic preparation on independent and contended keys.
use redb::backends::InMemoryBackend;
use redb::{
    ConcurrentWriteError, Database, Durability, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, StorageBackend, TableDefinition,
};
use serde_json::{Value, json};
use std::hint::black_box;
use std::io;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
const VALUE_BYTES: usize = 150;
const REFERENCE_KEYS: usize = 1024;
const REFERENCE_BASE: u64 = 1024;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Memory,
    Durable,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Durable => "durable",
        }
    }
}

#[derive(Clone, Copy)]
enum Api {
    Classic,
    Concurrent,
}

impl Api {
    fn name(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Concurrent => "concurrent",
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Independent,
    HotKey,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::HotKey => "hot_key",
        }
    }

    fn counter_key(self, writer: usize) -> u64 {
        match self {
            Self::Independent => writer as u64,
            Self::HotKey => 0,
        }
    }

    fn counters(self, writers: usize) -> usize {
        match self {
            Self::Independent => writers,
            Self::HotKey => 1,
        }
    }
}

// Retain a handle for measuring the memory backend after the database closes.
#[derive(Clone, Debug)]
struct SharedMemory(Arc<InMemoryBackend>);

impl StorageBackend for SharedMemory {
    fn len(&self) -> io::Result<u64> {
        StorageBackend::len(self.0.as_ref())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        StorageBackend::read(self.0.as_ref(), offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        StorageBackend::set_len(self.0.as_ref(), len)
    }

    fn sync_data(&self) -> io::Result<()> {
        StorageBackend::sync_data(self.0.as_ref())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        StorageBackend::write(self.0.as_ref(), offset, data)
    }
}

fn number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|value| value.parse().expect("Expected a nonnegative integer"))
        .unwrap_or(default)
}

fn value(key: u64, counter: u64) -> [u8; VALUE_BYTES] {
    let mut result = [0; VALUE_BYTES];
    for (index, byte) in result[8..].iter_mut().enumerate() {
        *byte = key
            .wrapping_mul(0x9e3779b97f4a7c15)
            .wrapping_add(index as u64)
            .rotate_left(index as u32 % 64) as u8;
    }
    result[..8].copy_from_slice(&counter.to_le_bytes());
    result
}

fn increment(mut bytes: Vec<u8>) -> Vec<u8> {
    let counter = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    bytes[..8].copy_from_slice(&counter.checked_add(1).unwrap().to_le_bytes());
    bytes
}

struct Case {
    mode: Mode,
    api: Api,
    workload: Workload,
    writers: usize,
    transactions: usize,
    preparation_reads: usize,
    max_retries: usize,
}

struct WorkerResult {
    samples: Vec<u64>,
    conflicts: usize,
}

fn classic_attempt(db: &Database, references: &[u64], counter: u64) -> Result<(), redb::Error> {
    let mut txn = db.begin_write()?;
    txn.set_durability(Durability::Immediate)?;
    {
        let mut table = txn.open_table(TABLE)?;
        for key in references {
            let guard = table.get(key)?.unwrap();
            black_box(guard.value());
        }
        let updated = increment(table.get(counter)?.unwrap().value().to_vec());
        table.insert(counter, updated.as_slice())?;
    }
    txn.commit()?;
    Ok(())
}

fn concurrent_attempt(
    db: &Database,
    references: &[u64],
    counter: u64,
) -> Result<(), ConcurrentWriteError> {
    let mut txn = db
        .begin_concurrent_write()
        .map_err(ConcurrentWriteError::Transaction)?;
    txn.set_durability(Durability::Immediate)
        .map_err(ConcurrentWriteError::Durability)?;
    {
        let mut table = txn.open_table(TABLE).map_err(ConcurrentWriteError::Table)?;
        for key in references {
            let guard = table
                .get(key)
                .map_err(ConcurrentWriteError::Storage)?
                .unwrap();
            black_box(guard.value());
        }
        let updated = increment(
            table
                .get(counter)
                .map_err(ConcurrentWriteError::Storage)?
                .unwrap()
                .value()
                .to_vec(),
        );
        table
            .insert(counter, updated.as_slice())
            .map_err(ConcurrentWriteError::Storage)?;
    }
    txn.commit()
}

impl Case {
    fn worker(
        &self,
        db: &Database,
        barrier: &Barrier,
        writer: usize,
    ) -> Result<WorkerResult, String> {
        let mut result = WorkerResult {
            samples: Vec::with_capacity(self.transactions),
            conflicts: 0,
        };
        let counter = self.workload.counter_key(writer);
        barrier.wait();
        for index in 0..self.transactions {
            let timer = Instant::now();
            let start = index.wrapping_mul(37).wrapping_add(writer * 17);
            let references: Vec<_> = (0..self.preparation_reads)
                .map(|offset| {
                    REFERENCE_BASE + (start.wrapping_add(offset * 67) % REFERENCE_KEYS) as u64
                })
                .collect();
            match self.api {
                Api::Classic => {
                    classic_attempt(db, &references, counter).map_err(|error| error.to_string())?;
                }
                Api::Concurrent => {
                    let mut retries = 0;
                    loop {
                        // Construct each attempt here so every retry receives a fresh snapshot.
                        match concurrent_attempt(db, &references, counter) {
                            Ok(()) => break,
                            Err(ConcurrentWriteError::Conflict { table }) => {
                                result.conflicts += 1;
                                if retries == self.max_retries {
                                    return Err(format!(
                                        "Writer {writer}, transaction {index} exceeded {} retries on {table}",
                                        self.max_retries
                                    ));
                                }
                                retries += 1;
                            }
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                }
            }
            result
                .samples
                .push(timer.elapsed().as_nanos().try_into().unwrap());
        }
        Ok(result)
    }

    fn initialize(&self, db: &Database) {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for counter in 0..self.workload.counters(self.writers) {
                table
                    .insert(counter as u64, value(counter as u64, 0).as_slice())
                    .unwrap();
            }
            for index in 0..REFERENCE_KEYS {
                let key = REFERENCE_BASE + index as u64;
                table.insert(key, value(key, 0).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    fn verify(&self, db: &Database) {
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        let counters = self.workload.counters(self.writers);
        assert_eq!(table.len().unwrap(), (counters + REFERENCE_KEYS) as u64);
        for counter in 0..counters {
            let expected = match self.workload {
                Workload::Independent => self.transactions,
                Workload::HotKey => self.transactions * self.writers,
            };
            assert_eq!(
                table.get(counter as u64).unwrap().unwrap().value(),
                value(counter as u64, expected as u64)
            );
        }
        for index in 0..REFERENCE_KEYS {
            let key = REFERENCE_BASE + index as u64;
            assert_eq!(table.get(key).unwrap().unwrap().value(), value(key, 0));
        }
    }

    fn run(&self) -> Value {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.redb");
        let memory = SharedMemory(Arc::new(InMemoryBackend::new()));
        let mut db = match self.mode {
            Mode::Memory => Database::builder()
                .create_with_backend(memory.clone())
                .unwrap(),
            Mode::Durable => Database::create(&path).unwrap(),
        };
        self.initialize(&db);
        let barrier = Barrier::new(self.writers + 1);
        let (elapsed_ns, reports) = thread::scope(|scope| {
            let handles: Vec<_> = (0..self.writers)
                .map(|writer| {
                    let db = &db;
                    let barrier = &barrier;
                    scope.spawn(move || self.worker(db, barrier, writer))
                })
                .collect();
            let timer = Instant::now();
            barrier.wait();
            let reports: Vec<_> = handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .expect("Writer panicked")
                        .expect("Writer failed")
                })
                .collect();
            let elapsed_ns: u64 = timer.elapsed().as_nanos().try_into().unwrap();
            (elapsed_ns, reports)
        });
        let conflicts: usize = reports.iter().map(|report| report.conflicts).sum();
        let mut samples: Vec<_> = reports
            .into_iter()
            .flat_map(|report| report.samples)
            .collect();
        samples.sort_unstable();
        let at = |percentile: usize| {
            samples[(samples.len() * percentile)
                .div_ceil(1000)
                .saturating_sub(1)]
        };
        self.verify(&db);
        assert!(db.check_integrity().unwrap());
        drop(db);
        if matches!(self.mode, Mode::Durable) {
            db = Database::open(&path).unwrap();
            self.verify(&db);
            assert!(db.check_integrity().unwrap());
            drop(db);
        }
        let backend_bytes = match self.mode {
            Mode::Memory => memory.len().unwrap(),
            Mode::Durable => std::fs::metadata(&path).unwrap().len(),
        };
        let transactions = self.transactions * self.writers;
        let transactions_per_second: u64 = (transactions as u128 * 1_000_000_000
            / u128::from(elapsed_ns))
        .try_into()
        .unwrap();
        json!({
            "mode": self.mode.name(),
            "api": self.api.name(),
            "workload": self.workload.name(),
            "writers": self.writers,
            "transactions": transactions,
            "attempts": transactions + conflicts,
            "conflicts": conflicts,
            "retries": conflicts,
            "elapsed_ns": elapsed_ns,
            "transactions_per_second": transactions_per_second,
            "latency_ns": {"p50": at(500), "p99": at(990), "p999": at(999), "max": samples.last().unwrap()},
            "backend_bytes": backend_bytes,
        })
    }
}

fn main() {
    let transactions = number("REDB_CONCURRENT_TRANSACTIONS", 1024);
    assert!(transactions > 0 && transactions.checked_mul(8).is_some());
    let preparation_reads = number("REDB_CONCURRENT_PREPARATION_READS", 64);
    assert!(preparation_reads <= REFERENCE_KEYS);
    let max_retries = number("REDB_CONCURRENT_MAX_RETRIES", 16_384);
    let modes: Vec<_> = std::env::var("REDB_CONCURRENT_MODES")
        .unwrap_or_else(|_| "memory,durable".into())
        .split(',')
        .map(|mode| match mode {
            "memory" => Mode::Memory,
            "durable" => Mode::Durable,
            _ => panic!("Expected memory,durable or a subset"),
        })
        .collect();
    assert!(
        modes
            .iter()
            .enumerate()
            .all(|(index, mode)| !modes[..index].contains(mode))
    );
    let writers: Vec<usize> = std::env::var("REDB_CONCURRENT_WRITERS")
        .unwrap_or_else(|_| "1,2,4".into())
        .split(',')
        .map(|writers| writers.parse().expect("Expected writer counts"))
        .collect();
    assert!(writers.iter().all(|writers| [1, 2, 4, 8].contains(writers)));
    assert!(
        writers
            .iter()
            .enumerate()
            .all(|(index, count)| !writers[..index].contains(count))
    );
    let mut results = Vec::new();
    let mut case_index = 0_usize;
    for &mode in &modes {
        for workload in [Workload::Independent, Workload::HotKey] {
            for &writers in &writers {
                let apis = if case_index.is_multiple_of(2) {
                    [Api::Classic, Api::Concurrent]
                } else {
                    [Api::Concurrent, Api::Classic]
                };
                case_index += 1;
                for api in apis {
                    let row = Case {
                        mode,
                        api,
                        workload,
                        writers,
                        transactions,
                        preparation_reads,
                        max_retries,
                    }
                    .run();
                    println!("{row}");
                    results.push(row);
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("REDB_CONCURRENT_OUTPUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "schema": 1,
                "transactions_per_writer": transactions,
                "preparation_reads": preparation_reads,
                "reference_keys": REFERENCE_KEYS,
                "value_bytes": VALUE_BYTES,
                "max_retries": max_retries,
                "modes": modes.iter().map(|mode| mode.name()).collect::<Vec<_>>(),
                "writers": writers,
                "results": results,
            }))
            .unwrap(),
        )
        .unwrap();
    }
}
