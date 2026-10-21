#![allow(dead_code)]

#[cfg(feature = "experimental-api-5")]
use redb::ReadableTable;
use redb::{
    ConcurrentWriteError, ConcurrentWriteTransaction, Database, GroupCommitConfig,
    GroupCommitStats, ReadableDatabase, StorageBackend, TableDefinition,
};
use std::collections::BTreeMap;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const DATA: TableDefinition<u64, u64> = TableDefinition::new("data");
pub const OTHER: TableDefinition<u64, u64> = TableDefinition::new("other");
const BLOCKER: TableDefinition<u64, u64> = TableDefinition::new("blocker");
const TIMEOUT: Duration = Duration::from_secs(30);

pub type Prepare = Box<dyn FnOnce(&mut ConcurrentWriteTransaction<'_>) + Send>;
pub type Outcome = std::thread::Result<Result<(), ConcurrentWriteError>>;

#[derive(Clone, Copy, Debug)]
pub enum SyncFault {
    ErrorBefore,
    ErrorAfter,
    PanicBefore,
    PanicAfter,
}

#[derive(Debug, Default)]
struct Images {
    live: Vec<u8>,
    durable: Vec<u8>,
}

#[derive(Debug, Default)]
struct PauseState {
    released_through: usize,
    released_all: bool,
}

#[derive(Debug)]
struct Pause {
    entered: mpsc::Sender<usize>,
    state: Mutex<PauseState>,
    changed: Condvar,
}

impl Pause {
    fn release_through(&self, sync: usize) {
        let mut state = self.state.lock().unwrap();
        state.released_through = state.released_through.max(sync);
        self.changed.notify_all();
    }

    fn release_all(&self) {
        self.state.lock().unwrap().released_all = true;
        self.changed.notify_all();
    }
}

#[derive(Debug, Default)]
struct Control {
    pause: Option<Arc<Pause>>,
    faults: BTreeMap<usize, SyncFault>,
    write_panic_pattern: Option<[u8; 8]>,
}

#[derive(Clone, Debug, Default)]
pub struct ControlledBackend {
    images: Arc<Mutex<Images>>,
    control: Arc<Mutex<Control>>,
    syncs: Arc<AtomicUsize>,
}

impl ControlledBackend {
    pub fn syncs(&self) -> usize {
        self.syncs.load(Ordering::Acquire)
    }

    pub fn pause_syncs(&self) -> PausedSyncs {
        let (entered, events) = mpsc::channel();
        let pause = Arc::new(Pause {
            entered,
            state: Mutex::new(PauseState::default()),
            changed: Condvar::new(),
        });
        self.control.lock().unwrap().pause = Some(pause.clone());
        PausedSyncs { pause, events }
    }

    pub fn fault_at(&self, sync: usize, fault: SyncFault) {
        assert!(
            self.control
                .lock()
                .unwrap()
                .faults
                .insert(sync, fault)
                .is_none()
        );
    }

    pub fn panic_on_write_pattern(&self, pattern: [u8; 8]) {
        self.control.lock().unwrap().write_panic_pattern = Some(pattern);
    }

    pub fn crash_image(&self) -> Self {
        let durable = self.images.lock().unwrap().durable.clone();
        Self {
            images: Arc::new(Mutex::new(Images {
                live: durable.clone(),
                durable,
            })),
            ..Self::default()
        }
    }
}

impl StorageBackend for ControlledBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(self.images.lock().unwrap().live.len().try_into().unwrap())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let offset = usize::try_from(offset).unwrap();
        let images = self.images.lock().unwrap();
        let bytes = images
            .live
            .get(offset..offset + out.len())
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        out.copy_from_slice(bytes);
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.images
            .lock()
            .unwrap()
            .live
            .resize(len.try_into().unwrap(), 0);
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        let panic = {
            let mut control = self.control.lock().unwrap();
            if offset >= 512
                && control.write_panic_pattern.is_some_and(|pattern| {
                    data.windows(pattern.len()).any(|bytes| bytes == pattern)
                })
            {
                control.write_panic_pattern.take();
                true
            } else {
                false
            }
        };
        assert!(!panic, "injected backend write panic");
        let offset = usize::try_from(offset).unwrap();
        let mut images = self.images.lock().unwrap();
        let bytes = images
            .live
            .get_mut(offset..offset + data.len())
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        bytes.copy_from_slice(data);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let sync = self.syncs.fetch_add(1, Ordering::AcqRel) + 1;
        let pause = self.control.lock().unwrap().pause.clone();
        if let Some(pause) = pause {
            let mut state = pause.state.lock().unwrap();
            if !state.released_all {
                pause.entered.send(sync).unwrap();
                while !state.released_all && state.released_through < sync {
                    state = pause.changed.wait(state).unwrap();
                }
            }
        }
        let fault = self.control.lock().unwrap().faults.remove(&sync);
        match fault {
            Some(SyncFault::ErrorBefore) => {
                return Err(io::Error::other("injected error before sync"));
            }
            Some(SyncFault::PanicBefore) => panic!("injected panic before sync"),
            _ => {}
        }
        {
            let mut images = self.images.lock().unwrap();
            images.durable = images.live.clone();
        }
        match fault {
            Some(SyncFault::ErrorAfter) => Err(io::Error::other("injected error after sync")),
            Some(SyncFault::PanicAfter) => panic!("injected panic after sync"),
            _ => Ok(()),
        }
    }
}

pub struct PausedSyncs {
    pause: Arc<Pause>,
    events: mpsc::Receiver<usize>,
}

impl PausedSyncs {
    pub fn next_sync(&self) -> usize {
        self.events
            .recv_timeout(TIMEOUT)
            .expect("a physical commit did not reach its next sync")
    }

    pub fn release_through(&self, sync: usize) {
        self.pause.release_through(sync);
    }

    pub fn release_all(&self) {
        self.pause.release_all();
    }
}

impl Drop for PausedSyncs {
    fn drop(&mut self) {
        self.release_all();
    }
}

pub fn new_database(config: GroupCommitConfig) -> (Arc<Database>, ControlledBackend) {
    let backend = ControlledBackend::default();
    let mut builder = Database::builder();
    builder.set_group_commit(config);
    let db = Arc::new(builder.create_with_backend(backend.clone()).unwrap());
    let initial = db.begin_write().unwrap();
    {
        let mut table = initial.open_table(DATA).unwrap();
        for key in 0..128 {
            table.insert(key, key * 10).unwrap();
        }
    }
    initial.open_table(OTHER).unwrap();
    initial.open_table(BLOCKER).unwrap().insert(0, 0).unwrap();
    initial.commit().unwrap();
    (db, backend)
}

pub fn read(db: &Database, table: TableDefinition<u64, u64>, key: u64) -> Option<u64> {
    db.begin_read()
        .unwrap()
        .open_table(table)
        .unwrap()
        .get(key)
        .unwrap()
        .map(|value| value.value())
}

pub fn wait_for_stats(db: &Database, predicate: impl Fn(GroupCommitStats) -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let stats = db.group_commit_stats().unwrap();
        if predicate(stats) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "coordinator did not reach the expected state"
        );
        std::thread::yield_now();
    }
}

pub fn spawn_commit(
    db: Arc<Database>,
    id: usize,
    prepare: Prepare,
    completed: mpsc::Sender<(usize, Outcome)>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut txn = db.begin_concurrent_write().unwrap();
            prepare(&mut txn);
            txn.commit()
        }));
        completed.send((id, result)).unwrap();
    })
}

// The first leader is held at a real backend sync so every follower has enqueued
// before the coordinator selects the next group. No scheduling delay forms the batch.
pub struct QueuedGroup {
    pub db: Arc<Database>,
    pub backend: ControlledBackend,
    pub paused: PausedSyncs,
    pub blocked_sync: usize,
    completed: mpsc::Receiver<(usize, Outcome)>,
    workers: Vec<JoinHandle<()>>,
    initial_batches: usize,
    results: Vec<(usize, Outcome)>,
}

impl QueuedGroup {
    pub fn new(db: Arc<Database>, backend: ControlledBackend, followers: Vec<Prepare>) -> Self {
        assert!(!followers.is_empty());
        let paused = backend.pause_syncs();
        let initial_batches = db.group_commit_stats().unwrap().physical_batches;
        let (completed, receiver) = mpsc::channel();
        let leader = spawn_commit(
            db.clone(),
            0,
            Box::new(|txn| {
                let mut table = txn.open_table(BLOCKER).unwrap();
                let value = table.get(0).unwrap().unwrap().value();
                table.insert(0, value + 1).unwrap();
            }),
            completed.clone(),
        );
        let blocked_sync = paused.next_sync();
        let count = followers.len();
        let mut workers = vec![leader];
        for (index, prepare) in followers.into_iter().enumerate() {
            workers.push(spawn_commit(
                db.clone(),
                index + 1,
                prepare,
                completed.clone(),
            ));
            // This also fixes the FIFO order used by dependency tests.
            wait_for_stats(&db, |stats| stats.queued_transactions == index + 1);
        }
        drop(completed);
        assert_eq!(db.group_commit_stats().unwrap().queued_transactions, count);
        Self {
            db,
            backend,
            paused,
            blocked_sync,
            completed: receiver,
            workers,
            initial_batches,
            results: Vec::new(),
        }
    }

    pub fn advance_to_batch(&mut self, transactions: usize) {
        for _ in 0..32 {
            let stats = self.db.group_commit_stats().unwrap();
            if stats.physical_batches > self.initial_batches
                && stats.active_transactions == transactions
            {
                return;
            }
            self.paused.release_through(self.blocked_sync);
            self.blocked_sync = self.paused.next_sync();
        }
        panic!("follower group did not reach a physical publication");
    }

    pub fn assert_followers_not_acknowledged(&mut self) {
        while let Ok(result) = self.completed.try_recv() {
            assert_eq!(result.0, 0, "follower acknowledged before its durable sync");
            self.results.push(result);
        }
    }

    pub fn finish(mut self) -> Vec<Outcome> {
        self.paused.release_all();
        while self.results.len() < self.workers.len() {
            self.results.push(
                self.completed
                    .recv_timeout(TIMEOUT)
                    .expect("group commit stranded a caller"),
            );
        }
        for worker in self.workers {
            worker.join().unwrap();
        }
        self.results.sort_by_key(|(id, _)| *id);
        let mut results: Vec<_> = self.results.into_iter().map(|(_, result)| result).collect();
        results.remove(0).unwrap().unwrap();
        results
    }
}

pub fn assert_success(result: Outcome) {
    result.expect("commit panicked").unwrap();
}

pub fn assert_conflict(result: Outcome) {
    assert!(matches!(
        result.expect("commit panicked"),
        Err(ConcurrentWriteError::Conflict { .. })
    ));
}
