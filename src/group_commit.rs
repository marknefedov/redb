//! Bounded, caller-led durable publication for prepared transactions.

use crate::commit_metrics::CommitPhase;
use crate::concurrent::PreparedWrite;
use crate::{ConcurrentWriteError, Database};
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::mem::size_of;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

/// Limits for optional group commit of immediate optimistic transactions.
///
/// Groups contain only already queued requests; there is no collection delay or background
/// thread. The elected caller can execute another request's codecs and comparators on its
/// thread. Batch and queue byte limits cover accounted prepared data, including read
/// preimages and metadata, rather than process RSS or pages pinned by snapshots. A request
/// exceeding either byte limit uses the ordinary optimistic commit path by itself.
#[derive(Clone, Copy, Debug)]
pub struct GroupCommitConfig {
    pub(crate) batch_transactions: usize,
    pub(crate) batch_bytes: usize,
    pub(crate) queue_transactions: usize,
    pub(crate) queue_bytes: usize,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            batch_transactions: 16,
            batch_bytes: 1024 * 1024,
            queue_transactions: 64,
            queue_bytes: 8 * 1024 * 1024,
        }
    }
}

impl GroupCommitConfig {
    /// Set the maximum selected requests per physical write transaction. Must be positive.
    #[must_use]
    pub fn with_max_batch_transactions(mut self, transactions: usize) -> Self {
        assert!(transactions > 0);
        self.batch_transactions = transactions;
        self
    }

    /// Set the maximum accounted prepared bytes per group. Must be positive.
    #[must_use]
    pub fn with_max_batch_bytes(mut self, bytes: usize) -> Self {
        assert!(bytes > 0);
        self.batch_bytes = bytes;
        self
    }

    /// Set the maximum queued requests, excluding the active group. Must be positive.
    #[must_use]
    pub fn with_max_queue_transactions(mut self, transactions: usize) -> Self {
        assert!(transactions > 0);
        self.queue_transactions = transactions;
        self
    }

    /// Set the maximum queued accounted prepared bytes, excluding the active group.
    /// Must be positive. A full queue reports [`ConcurrentWriteError::QueueFull`].
    #[must_use]
    pub fn with_max_queue_bytes(mut self, bytes: usize) -> Self {
        assert!(bytes > 0);
        self.queue_bytes = bytes;
        self
    }
}

/// A consistent snapshot of this handle's coordinator, available without timing metrics.
///
/// Queued and active limits are separate. Byte counts include prepared buffers and a
/// conservative metadata allowance; they exclude shared cached pages and allocator overhead.
#[derive(Clone, Copy, Debug, Default)]
pub struct GroupCommitStats {
    pub queued_transactions: usize,
    pub queued_bytes: usize,
    pub active_transactions: usize,
    pub active_bytes: usize,
    pub max_queued_transactions: usize,
    pub max_queued_bytes: usize,
    pub max_batch_transactions: usize,
    pub max_batch_bytes: usize,
    /// Successful grouped write transactions, including singleton groups. A cleanup epilogue
    /// may publish an additional root or perform additional synchronization.
    pub physical_batches: usize,
    /// Successful logical transactions, including validated read-only requests.
    pub committed_transactions: usize,
}

struct RequestState {
    prepared: Option<PreparedWrite>,
    result: Option<Result<(), ConcurrentWriteError>>,
    completed: bool,
    leader: bool,
    queued_at: Option<Instant>,
}

struct Request {
    bytes: usize,
    state: Mutex<RequestState>,
    ready: Condvar,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // These locks protect ownership and scheduling only. No user codec runs while held.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Request {
    fn finish(&self, result: Result<(), ConcurrentWriteError>) {
        let unused = {
            let mut state = lock(&self.state);
            if state.completed {
                return;
            }
            state.completed = true;
            state.leader = false;
            state.result = Some(result);
            state.prepared.take()
        };
        self.ready.notify_all();
        drop(unused);
    }
}

#[derive(Default)]
struct CoordinatorState {
    queue: VecDeque<Arc<Request>>,
    running: bool,
    stats: GroupCommitStats,
}

pub(crate) struct Coordinator {
    config: GroupCommitConfig,
    state: Mutex<CoordinatorState>,
}

impl Coordinator {
    pub(crate) fn new(config: GroupCommitConfig) -> Self {
        Self {
            config,
            state: Mutex::new(CoordinatorState::default()),
        }
    }

    pub(crate) fn stats(&self) -> GroupCommitStats {
        lock(&self.state).stats
    }

    pub(crate) fn commit(
        &self,
        database: &Database,
        prepared: PreparedWrite,
    ) -> Result<(), ConcurrentWriteError> {
        let memory = database.get_memory();
        let metrics = memory.metrics_state();
        let bytes = prepared
            .accounted_bytes()
            .saturating_add(size_of::<Request>())
            .saturating_add(2 * size_of::<usize>());
        if bytes > self.config.batch_bytes || bytes > self.config.queue_bytes {
            let result = prepared.commit_alone(database);
            if matches!(&result, Err(ConcurrentWriteError::Conflict { .. })) {
                metrics.record_conflict();
            }
            return result;
        }
        let request = Arc::new(Request {
            bytes,
            state: Mutex::new(RequestState {
                prepared: Some(prepared),
                result: None,
                completed: false,
                leader: false,
                queued_at: metrics.is_enabled().then(Instant::now),
            }),
            ready: Condvar::new(),
        });
        {
            let mut state = lock(&self.state);
            let queued_bytes = state.stats.queued_bytes.checked_add(bytes);
            if state.queue.len() >= self.config.queue_transactions
                || queued_bytes.is_none_or(|total| total > self.config.queue_bytes)
            {
                metrics.record_queue_rejection();
                return Err(ConcurrentWriteError::QueueFull);
            }
            if !state.running {
                state.running = true;
                lock(&request.state).leader = true;
            }
            state.queue.push_back(request.clone());
            state.stats.queued_transactions = state.queue.len();
            state.stats.queued_bytes = queued_bytes.unwrap();
            state.stats.max_queued_transactions = state
                .stats
                .max_queued_transactions
                .max(state.stats.queued_transactions);
            state.stats.max_queued_bytes =
                state.stats.max_queued_bytes.max(state.stats.queued_bytes);
        }
        loop {
            let mut own = lock(&request.state);
            if let Some(result) = own.result.take() {
                return result;
            }
            if own.leader {
                own.leader = false;
                drop(own);
                self.lead(database);
            } else {
                drop(
                    request
                        .ready
                        .wait(own)
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                );
            }
        }
    }

    fn lead(&self, database: &Database) {
        // Covers dequeue, codecs, storage, history and result publication, not only commit.
        // During unwinding every owned or queued waiter receives an uncertain error.
        let mut guard = LeaderGuard {
            coordinator: self,
            requests: Vec::new(),
            armed: true,
        };
        {
            let mut state = lock(&self.state);
            let mut bytes = 0usize;
            while let Some(request) = state.queue.front() {
                let Some(next_bytes) = bytes.checked_add(request.bytes) else {
                    break;
                };
                if guard.requests.len() >= self.config.batch_transactions
                    || next_bytes > self.config.batch_bytes
                {
                    break;
                }
                let request = state.queue.pop_front().unwrap();
                bytes = next_bytes;
                state.stats.queued_bytes -= request.bytes;
                guard.requests.push(request);
            }
            state.stats.queued_transactions = state.queue.len();
            state.stats.active_transactions = guard.requests.len();
            state.stats.active_bytes = bytes;
            state.stats.max_batch_transactions =
                state.stats.max_batch_transactions.max(guard.requests.len());
            state.stats.max_batch_bytes = state.stats.max_batch_bytes.max(bytes);
        }
        let memory = database.get_memory();
        let mut prepared = Vec::with_capacity(guard.requests.len());
        for request in &guard.requests {
            let mut state = lock(&request.state);
            if let Some(start) = state.queued_at.take() {
                memory
                    .metrics_state()
                    .record_elapsed(CommitPhase::QueueWait, start.elapsed());
            }
            prepared.push(state.prepared.take().unwrap());
        }
        let outcome = PreparedWrite::commit_batch(database, prepared);
        let committed = outcome
            .results
            .iter()
            .filter(|result| result.is_ok())
            .count();
        {
            let mut state = lock(&self.state);
            state.stats.committed_transactions =
                state.stats.committed_transactions.saturating_add(committed);
            if outcome.published {
                state.stats.physical_batches = state.stats.physical_batches.saturating_add(1);
            }
        }
        if outcome.published {
            memory.metrics_state().record_group(committed);
        }
        for (request, result) in guard.requests.iter().zip(outcome.results) {
            request.finish(result);
        }
        {
            let mut state = lock(&self.state);
            state.stats.active_transactions = 0;
            state.stats.active_bytes = 0;
            if let Some(next) = state.queue.front() {
                let mut next_state = lock(&next.state);
                next_state.leader = true;
                next.ready.notify_all();
            } else {
                state.running = false;
            }
        }
        guard.armed = false;
    }
}

struct LeaderGuard<'a> {
    coordinator: &'a Coordinator,
    requests: Vec<Arc<Request>>,
    armed: bool,
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let queued = {
            let mut state = lock(&self.coordinator.state);
            state.running = false;
            state.stats.queued_transactions = 0;
            state.stats.queued_bytes = 0;
            state.stats.active_transactions = 0;
            state.stats.active_bytes = 0;
            core::mem::take(&mut state.queue)
        };
        for request in self.requests.iter().chain(queued.iter()) {
            request.finish(Err(ConcurrentWriteError::BatchPanicked));
        }
    }
}
