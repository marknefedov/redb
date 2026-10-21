//! Repeatable size and latency workloads. No redb compaction is performed.
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use serde_json::json;
use std::path::Path;
use std::time::Instant;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("data");
const BATCH: usize = 256;

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

fn bytes(path: &Path) -> u64 {
    if path.is_dir() {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| bytes(&entry.unwrap().path()))
            .sum()
    } else {
        match std::fs::metadata(path) {
            Ok(metadata) => metadata.len(),
            // Background compaction can remove a file after read_dir returned it.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("Cannot measure {}: {error}", path.display()),
        }
    }
}

fn latency(samples: &mut [u64]) -> serde_json::Value {
    samples.sort_unstable();
    let at = |n: usize| samples[(samples.len() * n).div_ceil(1000).saturating_sub(1)];
    json!({"p50_us": at(500), "p99_us": at(990), "p999_us": at(999), "max_us": samples.last().unwrap()})
}

fn main() {
    let count: usize = std::env::var("REDB_SPACE_ENTRIES")
        .unwrap_or_else(|_| "512000".into())
        .parse()
        .unwrap();
    assert!(count >= 1024 && count.is_multiple_of(2));
    let mut results = vec![];
    for size in [32, 150, 1024, 4096] {
        let directory = tempfile::tempdir().unwrap();
        let redb_path = directory.path().join("data.redb");
        let rocks_path = directory.path().join("rocksdb");
        let mut redb = Database::create(&redb_path).unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let rocks = DB::open(&options, &rocks_path).unwrap();
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        let mut live_start = 0;
        let mut live_end = 0;
        for phase in ["load", "delete_half", "churn_1", "churn_2"] {
            let operations = if phase == "load" { count } else { count / 2 };
            let mut redb_samples = vec![];
            let mut rocks_samples = vec![];
            for start in (0..operations).step_by(BATCH) {
                let end = (start + BATCH).min(operations);
                let pairs: Vec<_> = (start..end)
                    .map(|i| {
                        let index = if phase == "load" { i } else { live_end + i };
                        (key(index), value(index, size))
                    })
                    .collect();
                let removes: Vec<_> = (start..end).map(|i| key(live_start + i)).collect();
                let timer = Instant::now();
                let txn = redb.begin_write().unwrap();
                {
                    let mut table = txn.open_table(TABLE).unwrap();
                    if phase != "load" {
                        for key in &removes {
                            table.remove(key.as_slice()).unwrap();
                        }
                    }
                    if phase != "delete_half" {
                        for (key, value) in &pairs {
                            table.insert(key.as_slice(), value.as_slice()).unwrap();
                        }
                    }
                }
                txn.commit().unwrap();
                redb_samples.push(timer.elapsed().as_micros() as u64);
                let timer = Instant::now();
                let mut batch = WriteBatch::default();
                if phase != "load" {
                    for key in &removes {
                        batch.delete(key);
                    }
                }
                if phase != "delete_half" {
                    for (key, value) in &pairs {
                        batch.put(key, value);
                    }
                }
                rocks.write_opt(batch, &write_options).unwrap();
                rocks_samples.push(timer.elapsed().as_micros() as u64);
            }
            if phase != "load" {
                live_start += operations;
            }
            if phase != "delete_half" {
                live_end += operations;
            }
            // Flush the memtable before comparing sizes, then record the compacted RocksDB
            // reference separately. Neither flush nor compaction is included in write latency.
            rocks.flush().unwrap();
            let rocks_online_bytes = bytes(&rocks_path);
            rocks.compact_range::<&[u8], &[u8]>(None, None);
            let rocks_bytes = bytes(&rocks_path);
            let txn = redb.begin_write().unwrap();
            let stats = txn.stats().unwrap();
            let row = json!({
                "value_bytes": size, "phase": phase, "entries": live_end - live_start,
                "payload_bytes": (live_end - live_start) * (24 + size),
                "redb_bytes": bytes(&redb_path), "rocksdb_online_bytes": rocks_online_bytes,
                "rocksdb_compacted_bytes": rocks_bytes,
                "redb_stored_bytes": stats.stored_bytes(),
                "redb_metadata_bytes": stats.metadata_bytes(),
                "redb_fragmented_bytes": stats.fragmented_bytes(),
                "redb_latency": latency(&mut redb_samples), "rocksdb_latency": latency(&mut rocks_samples),
            });
            txn.abort().unwrap();
            println!("{row}");
            results.push(row);
        }
        drop(rocks);
        drop(redb);
        redb = Database::open(&redb_path).unwrap();
        let txn = redb.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), (live_end - live_start) as u64);
        for index in live_start..live_end {
            assert_eq!(
                table.get(key(index).as_slice()).unwrap().unwrap().value(),
                value(index, size)
            );
        }
        drop(table);
        drop(txn);
        assert!(redb.check_integrity().unwrap());
    }
    if let Ok(path) = std::env::var("REDB_SPACE_OUTPUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(
                &json!({"schema": 1, "entries": count, "batch_size": BATCH, "rocksdb_compression": "lz4", "results": results}),
            )
            .unwrap(),
        )
        .unwrap();
    }
}
