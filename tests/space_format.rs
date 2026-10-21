#[cfg(feature = "experimental-api-5")]
use redb::ReadableTable;
use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use std::collections::BTreeMap;

const DATA: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
const LEGACY_DATA: redb2_6::TableDefinition<u64, &[u8]> = redb2_6::TableDefinition::new("data");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn value(key: u64, generation: u8, len: usize) -> Vec<u8> {
    let mut result = vec![generation; len];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte ^= key.to_le_bytes()[index % 8];
    }
    result
}

fn check_contents(db: &Database, expected: &BTreeMap<u64, Vec<u8>>) {
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(DATA).unwrap();
    assert_eq!(table.len().unwrap(), expected.len() as u64);
    for (&key, value) in expected {
        assert_eq!(table.get(key).unwrap().unwrap().value(), value);
    }
}

// Existing v3 files retain their allocation geometry across ordinary writes and reopen.
#[test]
fn legacy_database_retains_its_geometry_through_writes_and_reopen() {
    let file = create_tempfile();
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(file.path())
            .unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LEGACY_DATA).unwrap();
            for key in 0..1024 {
                table.insert(key, value(key, 1, 150).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let geometry = std::fs::read(file.path()).unwrap()[12..24].to_vec();
    let expected: BTreeMap<_, _> = (512..1536)
        .map(|key| (key, value(key, 2, if key % 3 == 0 { 4352 } else { 257 })))
        .collect();
    {
        let mut db = Database::open(file.path()).unwrap();
        let mut txn = db.begin_write().unwrap();
        txn.set_quick_repair(true);
        {
            let mut table = txn.open_table(DATA).unwrap();
            for key in 0..512 {
                table.remove(key).unwrap();
            }
            for (&key, value) in &expected {
                table.insert(key, value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        assert!(db.check_integrity().unwrap());
    }

    assert_legacy_geometry(file.path(), &geometry);
    {
        let mut db = Database::open(file.path()).unwrap();
        check_contents(&db, &expected);
        assert!(db.check_integrity().unwrap());
        let txn = db.begin_write().unwrap();
        txn.open_table(DATA)
            .unwrap()
            .insert(512, b"written after reopen".as_slice())
            .unwrap();
        txn.commit().unwrap();
    }

    assert_legacy_geometry(file.path(), &geometry);
    let mut expected = expected;
    expected.insert(512, b"written after reopen".to_vec());
    let mut db = Database::open(file.path()).unwrap();
    check_contents(&db, &expected);
    assert!(db.check_integrity().unwrap());
}

fn assert_legacy_geometry(path: &std::path::Path, geometry: &[u8]) {
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(&bytes[12..24], geometry);
    assert_eq!(bytes[64], 3);
    assert_eq!(bytes[192], 3);
}

fn damage_inactive_version(path: &std::path::Path, version: u8) {
    let mut bytes = std::fs::read(path).unwrap();
    let primary = usize::from(bytes[9] & 1);
    bytes[64 + (1 - primary) * 128] = version;
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn damaged_inactive_version_does_not_upgrade_a_legacy_database() {
    let file = create_tempfile();
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(file.path())
            .unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(LEGACY_DATA)
            .unwrap()
            .insert(1, b"before damage".as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let geometry = std::fs::read(file.path()).unwrap()[12..24].to_vec();
    damage_inactive_version(file.path(), 4);
    {
        let mut db = Database::open(file.path()).unwrap();
        let mut expected = BTreeMap::from([(1, b"before damage".to_vec())]);
        check_contents(&db, &expected);
        let txn = db.begin_write().unwrap();
        txn.open_table(DATA)
            .unwrap()
            .insert(1, b"after damage".as_slice())
            .unwrap();
        txn.commit().unwrap();
        expected.insert(1, b"after damage".to_vec());
        check_contents(&db, &expected);
        assert!(db.check_integrity().unwrap());
    }
    assert_legacy_geometry(file.path(), &geometry);
    let mut db = Database::open(file.path()).unwrap();
    check_contents(&db, &BTreeMap::from([(1, b"after damage".to_vec())]));
    assert!(db.check_integrity().unwrap());
}

#[test]
fn damaged_inactive_version_does_not_downgrade_an_extent_database() {
    let file = create_tempfile();
    {
        let db = Database::create(file.path()).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(DATA)
            .unwrap()
            .insert(1, b"before damage".as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    damage_inactive_version(file.path(), 3);
    let expected = BTreeMap::from([(1, value(1, 2, 4352))]);
    {
        let db = Database::open(file.path()).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(DATA)
            .unwrap()
            .insert(1, expected[&1].as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let mut db = Database::open(file.path()).unwrap();
    check_contents(&db, &expected);
    assert!(db.check_integrity().unwrap());
    drop(db);
    let bytes = std::fs::read(file.path()).unwrap();
    assert_eq!(bytes[64], 4);
    assert_eq!(bytes[192], 4);
}

// Exercise both extent encodings through the same reader and persistent savepoint.
#[test]
fn exact_and_buddy_extents_survive_restore_abort_and_reopen() {
    const SIZES: [usize; 9] = [1, 255, 256, 257, 1500, 4096, 8191, 8192, 8193];
    for quick_repair in [false, true] {
        let file = create_tempfile();
        let mut expected: BTreeMap<_, _> = (0..256)
            .map(|key| (key, value(key, 1, SIZES[key as usize % SIZES.len()])))
            .collect();
        expected.insert(10000, value(10000, 1, 8 * 1024 * 1024 - 2048));
        expected.insert(10001, value(10001, 1, 8 * 1024 * 1024 + 512));
        let savepoint;
        {
            let db = Database::create(file.path()).unwrap();
            let mut txn = db.begin_write().unwrap();
            txn.set_quick_repair(quick_repair);
            {
                let mut table = txn.open_table(DATA).unwrap();
                for (&key, value) in &expected {
                    table.insert(key, value.as_slice()).unwrap();
                }
            }
            txn.commit().unwrap();
            let txn = db.begin_write().unwrap();
            savepoint = txn.persistent_savepoint().unwrap();
            txn.commit().unwrap();

            let reader = db.begin_read().unwrap();
            for generation in 2..10 {
                let mut txn = db.begin_write().unwrap();
                txn.set_quick_repair(quick_repair);
                {
                    let mut table = txn.open_table(DATA).unwrap();
                    for key in 0..256 {
                        table
                            .insert(
                                key,
                                value(key, generation, SIZES[(key as usize + 3) % SIZES.len()])
                                    .as_slice(),
                            )
                            .unwrap();
                    }
                    table.remove(10000).unwrap();
                    table.insert(10001, &[generation; 257][..]).unwrap();
                    table.insert(20000, &[generation; 1500][..]).unwrap();
                }
                txn.commit().unwrap();
            }
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(DATA).unwrap();
                table.remove(10001).unwrap();
                table
                    .insert(30000, value(30000, 10, 8 * 1024 * 1024 + 512).as_slice())
                    .unwrap();
            }
            txn.abort().unwrap();
            let table = reader.open_table(DATA).unwrap();
            assert_eq!(table.len().unwrap(), expected.len() as u64);
            for (&key, value) in &expected {
                assert_eq!(table.get(key).unwrap().unwrap().value(), value);
            }
        }

        let mut db = Database::open(file.path()).unwrap();
        {
            let txn = db.begin_read().unwrap();
            let table = txn.open_table(DATA).unwrap();
            assert!(table.get(10000).unwrap().is_none());
            assert!(table.get(30000).unwrap().is_none());
            assert_eq!(table.get(10001).unwrap().unwrap().value(), &[9; 257]);
        }
        let mut txn = db.begin_write().unwrap();
        let saved = txn.get_persistent_savepoint(savepoint).unwrap();
        txn.restore_savepoint(&saved).unwrap();
        drop(saved);
        txn.commit().unwrap();
        check_contents(&db, &expected);
        let txn = db.begin_write().unwrap();
        assert!(txn.delete_persistent_savepoint(savepoint).unwrap());
        txn.commit().unwrap();
        assert!(db.check_integrity().unwrap());
        drop(db);

        let mut db = Database::open(file.path()).unwrap();
        check_contents(&db, &expected);
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(DATA).unwrap();
            for key in 0..200 {
                table.remove(key).unwrap();
                expected.remove(&key);
            }
            let replacement = value(10000, 11, 150);
            table.insert(10000, replacement.as_slice()).unwrap();
            expected.insert(10000, replacement);
        }
        txn.commit().unwrap();
        for _ in 0..8 {
            db.begin_write().unwrap().commit().unwrap();
        }
        check_contents(&db, &expected);
        assert!(db.check_integrity().unwrap());
    }
}

#[test]
fn releasing_a_snapshot_truncates_the_file_in_bounded_steps() {
    let file = create_tempfile();
    let mut db = Database::create(file.path()).unwrap();
    let large = value(1, 1, 1024 * 1024);
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(DATA).unwrap();
        table.insert(0, b"retained".as_slice()).unwrap();
        for key in 1..33 {
            table.insert(key, large.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    let reader = db.begin_read().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(DATA).unwrap();
        for key in 1..33 {
            table.remove(key).unwrap();
        }
    }
    txn.commit().unwrap();
    assert_eq!(
        reader
            .open_table(DATA)
            .unwrap()
            .get(32)
            .unwrap()
            .unwrap()
            .value(),
        large
    );
    let before = file.as_file().metadata().unwrap().len();
    drop(reader);
    let mut previous = before;
    for _ in 0..64 {
        db.begin_write().unwrap().commit().unwrap();
        let current = file.as_file().metadata().unwrap().len();
        assert!(
            previous.saturating_sub(current) <= 1024 * 1024,
            "one commit shrank {previous} bytes to {current} bytes"
        );
        previous = current;
    }
    assert!(previous < before / 2, "{before} -> {previous}");
    check_contents(&db, &BTreeMap::from([(0, b"retained".to_vec())]));
    assert!(db.check_integrity().unwrap());
}
