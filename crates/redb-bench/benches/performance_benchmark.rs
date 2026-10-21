//! Deterministic CPU and durable-file workloads with transaction latency distributions.
use redb::backends::InMemoryBackend;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use serde_json::{Value, json};
use std::hint::black_box;
use std::time::Instant;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("data");

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

fn number(name: &str, default: usize) -> usize {
    let result = std::env::var(name)
        .map(|value| value.parse().expect("Expected a positive integer"))
        .unwrap_or(default);
    assert!(result > 0, "{name} must be positive");
    result
}

fn key(index: usize) -> [u8; 24] {
    let mut result = [0; 24];
    let mut state = index as u64;
    for chunk in result.chunks_exact_mut(8) {
        state = state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        chunk.copy_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    result
}

fn value(index: usize, size: usize) -> Vec<u8> {
    (0..size.div_ceil(24))
        .flat_map(|part| key(index.wrapping_mul(65537).wrapping_add(part)))
        .take(size)
        .collect()
}

fn elapsed(timer: Instant) -> u64 {
    timer.elapsed().as_nanos().try_into().unwrap()
}

struct Workload {
    round: usize,
    mode: Mode,
    value_bytes: usize,
    entries: usize,
    batch_size: usize,
}

impl Workload {
    fn result(
        &self,
        phase: &str,
        operations: usize,
        transactions: usize,
        mut samples: Vec<u64>,
    ) -> Value {
        let elapsed_ns: u64 = samples.iter().sum();
        samples.sort_unstable();
        let at = |percentile: usize| {
            samples[(samples.len() * percentile)
                .div_ceil(1000)
                .saturating_sub(1)]
        };
        let rate = |count: usize| -> u64 {
            (count as u128 * 1_000_000_000 / u128::from(elapsed_ns))
                .try_into()
                .unwrap()
        };
        json!({
            "round": self.round,
            "mode": self.mode.name(),
            "value_bytes": self.value_bytes,
            "phase": phase,
            "operations": operations,
            "transactions": transactions,
            "samples": samples.len(),
            "elapsed_ns": elapsed_ns,
            "ops_per_second": rate(operations),
            "transactions_per_second": rate(transactions),
            "latency_ns": {"p50": at(500), "p99": at(990), "p999": at(999), "max": samples.last().unwrap()},
        })
    }

    fn write(&self, db: &Database, churn: bool) -> Value {
        let mut samples = Vec::with_capacity(self.entries.div_ceil(self.batch_size));
        for start in (0..self.entries).step_by(self.batch_size) {
            let end = (start + self.batch_size).min(self.entries);
            let pairs: Vec<_> = (start..end)
                .map(|index| {
                    let index = if churn { self.entries + index } else { index };
                    (key(index), value(index, self.value_bytes))
                })
                .collect();
            let removes: Vec<_> = if churn {
                (start..end).map(key).collect()
            } else {
                Vec::new()
            };
            let timer = Instant::now();
            let mut txn = db.begin_write().unwrap();
            txn.set_durability(Durability::Immediate).unwrap();
            {
                let mut table = txn.open_table(TABLE).unwrap();
                for key in &removes {
                    table.remove(key.as_slice()).unwrap();
                }
                for (key, value) in &pairs {
                    table.insert(key.as_slice(), value.as_slice()).unwrap();
                }
            }
            txn.commit().unwrap();
            samples.push(elapsed(timer));
        }
        self.result(
            if churn { "churn" } else { "load" },
            self.entries * if churn { 2 } else { 1 },
            samples.len(),
            samples,
        )
    }

    fn read(&self, db: &Database) -> Value {
        let mut keys: Vec<_> = (self.entries..self.entries * 2).map(key).collect();
        fastrand::Rng::with_seed(3).shuffle(&mut keys);
        let mut samples = Vec::with_capacity(self.entries.div_ceil(self.batch_size));
        // Keep a single read transaction open so lookup timing does not include reopening it.
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        for batch in keys.chunks(self.batch_size) {
            let timer = Instant::now();
            for key in batch {
                let guard = table.get(key.as_slice()).unwrap().unwrap();
                black_box(guard.value());
            }
            samples.push(elapsed(timer));
        }
        self.result("random_read", self.entries, 1, samples)
    }

    fn verify(&self, db: &Database, start: usize) {
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), self.entries as u64);
        for index in start..start + self.entries {
            assert_eq!(
                table.get(key(index).as_slice()).unwrap().unwrap().value(),
                value(index, self.value_bytes)
            );
        }
    }
}

fn main() {
    let entries = number("REDB_PERF_ENTRIES", 65_536);
    assert!(entries.checked_mul(2).is_some(), "Too many entries");
    let batch_size = number("REDB_PERF_BATCH", 64);
    let repeats = number("REDB_PERF_REPEATS", 3);
    let modes: Vec<_> = std::env::var("REDB_PERF_MODES")
        .unwrap_or_else(|_| "memory,durable".into())
        .split(',')
        .map(|mode| match mode {
            "memory" => Mode::Memory,
            "durable" => Mode::Durable,
            _ => panic!("Expected REDB_PERF_MODES=memory,durable or a subset"),
        })
        .collect();
    let value_bytes: Vec<usize> = std::env::var("REDB_PERF_VALUES")
        .unwrap_or_else(|_| "150,4096".into())
        .split(',')
        .map(|size| size.parse().expect("Expected positive value sizes"))
        .collect();
    assert!(
        modes
            .iter()
            .enumerate()
            .all(|(index, mode)| !modes[..index].contains(mode)),
        "Duplicate benchmark mode"
    );
    assert!(value_bytes.iter().all(|&size| size > 0));
    assert!(
        value_bytes
            .iter()
            .enumerate()
            .all(|(index, size)| !value_bytes[..index].contains(size)),
        "Duplicate value size"
    );
    let mut results = Vec::new();
    for round in 1..=repeats {
        for &mode in &modes {
            for &size in &value_bytes {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("data.redb");
                let mut db = match mode {
                    Mode::Memory => Database::builder()
                        .create_with_backend(InMemoryBackend::new())
                        .unwrap(),
                    Mode::Durable => Database::create(&path).unwrap(),
                };
                let workload = Workload {
                    round,
                    mode,
                    value_bytes: size,
                    entries,
                    batch_size,
                };
                let load = workload.write(&db, false);
                println!("{load}");
                results.push(load);
                workload.verify(&db, 0);
                let churn = workload.write(&db, true);
                println!("{churn}");
                results.push(churn);
                let read = workload.read(&db);
                println!("{read}");
                results.push(read);
                workload.verify(&db, entries);
                assert!(db.check_integrity().unwrap());
                if matches!(mode, Mode::Durable) {
                    drop(db);
                    db = Database::open(&path).unwrap();
                    workload.verify(&db, entries);
                    assert!(db.check_integrity().unwrap());
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("REDB_PERF_OUTPUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "schema": 1,
                "entries": entries,
                "batch_size": batch_size,
                "repeats": repeats,
                "modes": modes.iter().map(|mode| mode.name()).collect::<Vec<_>>(),
                "value_bytes": value_bytes,
                "results": results,
            }))
            .unwrap(),
        )
        .unwrap();
    }
}
