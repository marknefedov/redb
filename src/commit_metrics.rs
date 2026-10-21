//! Optional cumulative measurements of commit work and backend I/O.

use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cumulative measurements of one commit phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitPhaseMetrics {
    /// Number of phase executions, including errors and panics.
    pub calls: u64,
    /// Total elapsed nanoseconds across executions.
    pub total_ns: u64,
    /// Longest execution in nanoseconds. This is not an additive counter.
    pub max_ns: u64,
}

/// Optional cumulative commit measurements for one database handle.
///
/// Collection is disabled by default. Enable it with
/// [`crate::Database::set_commit_metrics_enabled`]. Counters saturate instead of wrapping,
/// and disabling collection retains their values. Take snapshots while workers are idle
/// for mutually consistent counters; concurrent snapshots read each counter independently.
///
/// Phases overlap: `commit` includes backend `write` and `sync` work, and waiting times
/// accumulate across concurrent requests. Their totals must not be added into a time
/// breakdown. Backend measurements include calls made by cache pressure and maintenance,
/// as well as the commit itself. A cumulative maximum cannot be subtracted between snapshots.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitMetrics {
    /// Whether new phase executions are currently measured.
    pub enabled: bool,
    /// Time waiting to acquire the ordinary writer slot.
    pub writer_wait: CommitPhaseMetrics,
    /// Time submitted optimistic transactions spend waiting in the group coordinator.
    pub queue_wait: CommitPhaseMetrics,
    /// Time validating optimistic dependencies.
    pub validation: CommitPhaseMetrics,
    /// Time applying optimistic private changes to the physical write transaction.
    pub replay: CommitPhaseMetrics,
    /// Time executing physical write-transaction commits, including maintenance and I/O.
    pub commit: CommitPhaseMetrics,
    /// Time inside backend write calls.
    pub write: CommitPhaseMetrics,
    /// Time inside backend synchronization calls.
    pub sync: CommitPhaseMetrics,
    /// Successful physical write-transaction commits, including non-durable commits.
    pub physical_commits: u64,
    /// Successful physical commits with Immediate durability.
    pub durable_commits: u64,
    /// Physical commit attempts ending in an error or unwinding panic.
    pub commit_errors: u64,
    /// Bytes passed to backend write calls which returned success.
    /// Partial bytes written by a failing call cannot be determined.
    pub written_bytes: u64,
    /// Backend write calls ending in an error or unwinding panic.
    pub write_errors: u64,
    /// Backend synchronization calls ending in an error or unwinding panic.
    pub sync_errors: u64,
    /// Successful grouped physical commits, including singleton groups.
    pub group_batches: u64,
    /// Optimistic transactions acknowledged by those successful groups.
    pub grouped_transactions: u64,
    /// Largest successful group, in logical transactions.
    pub max_group_size: u64,
    /// Optimistic transactions rejected because a dependency changed.
    pub conflicts: u64,
    /// Transactions rejected by group admission limits.
    pub queue_rejections: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum CommitPhase {
    WriterWait,
    QueueWait,
    Validation,
    Replay,
    Commit,
    Write,
    Sync,
}

const PHASES: usize = 7;

#[derive(Debug, Default)]
struct AtomicPhaseMetrics {
    calls: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}

impl AtomicPhaseMetrics {
    fn record(&self, nanoseconds: u64) {
        add(&self.calls, 1);
        add(&self.total_ns, nanoseconds);
        self.max_ns.fetch_max(nanoseconds, Ordering::Relaxed);
    }

    fn snapshot(&self) -> CommitPhaseMetrics {
        CommitPhaseMetrics {
            calls: self.calls.load(Ordering::Relaxed),
            total_ns: self.total_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct MetricsState {
    enabled: AtomicBool,
    phases: [AtomicPhaseMetrics; PHASES],
    physical_commits: AtomicU64,
    durable_commits: AtomicU64,
    commit_errors: AtomicU64,
    written_bytes: AtomicU64,
    write_errors: AtomicU64,
    sync_errors: AtomicU64,
    group_batches: AtomicU64,
    grouped_transactions: AtomicU64,
    max_group_size: AtomicU64,
    conflicts: AtomicU64,
    queue_rejections: AtomicU64,
}

impl MetricsState {
    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub(crate) fn start(&self, phase: CommitPhase) -> Option<PhaseTimer<'_>> {
        self.is_enabled().then(|| PhaseTimer {
            state: self,
            phase,
            start: Instant::now(),
            elapsed: None,
            success: false,
        })
    }

    // For owned queue requests whose timer cannot borrow the metrics state.
    // Call only for a timestamp captured while collection was enabled.
    pub(crate) fn record_elapsed(&self, phase: CommitPhase, elapsed: Duration) {
        self.phases[phase as usize].record(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
    }

    pub(crate) fn snapshot(&self) -> CommitMetrics {
        let phase = |phase: CommitPhase| self.phases[phase as usize].snapshot();
        CommitMetrics {
            enabled: self.is_enabled(),
            writer_wait: phase(CommitPhase::WriterWait),
            queue_wait: phase(CommitPhase::QueueWait),
            validation: phase(CommitPhase::Validation),
            replay: phase(CommitPhase::Replay),
            commit: phase(CommitPhase::Commit),
            write: phase(CommitPhase::Write),
            sync: phase(CommitPhase::Sync),
            physical_commits: self.physical_commits.load(Ordering::Relaxed),
            durable_commits: self.durable_commits.load(Ordering::Relaxed),
            commit_errors: self.commit_errors.load(Ordering::Relaxed),
            written_bytes: self.written_bytes.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            sync_errors: self.sync_errors.load(Ordering::Relaxed),
            group_batches: self.group_batches.load(Ordering::Relaxed),
            grouped_transactions: self.grouped_transactions.load(Ordering::Relaxed),
            max_group_size: self.max_group_size.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            queue_rejections: self.queue_rejections.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_group(&self, transactions: usize) {
        if self.is_enabled() {
            let transactions = u64::try_from(transactions).unwrap_or(u64::MAX);
            add(&self.group_batches, 1);
            add(&self.grouped_transactions, transactions);
            self.max_group_size
                .fetch_max(transactions, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_conflict(&self) {
        if self.is_enabled() {
            add(&self.conflicts, 1);
        }
    }

    pub(crate) fn record_queue_rejection(&self) {
        if self.is_enabled() {
            add(&self.queue_rejections, 1);
        }
    }
}

pub(crate) struct PhaseTimer<'a> {
    state: &'a MetricsState,
    phase: CommitPhase,
    start: Instant,
    elapsed: Option<Duration>,
    success: bool,
}

impl PhaseTimer<'_> {
    pub(crate) fn complete(mut self, success: bool) {
        self.elapsed = Some(self.start.elapsed());
        self.success = success;
    }

    pub(crate) fn complete_write(mut self, success: bool, bytes: usize) {
        debug_assert!(matches!(self.phase, CommitPhase::Write));
        self.elapsed = Some(self.start.elapsed());
        self.success = success;
        if success {
            add(
                &self.state.written_bytes,
                u64::try_from(bytes).unwrap_or(u64::MAX),
            );
        }
    }

    pub(crate) fn complete_commit(mut self, success: bool, durable: bool) {
        debug_assert!(matches!(self.phase, CommitPhase::Commit));
        self.elapsed = Some(self.start.elapsed());
        self.success = success;
        if success && durable {
            add(&self.state.durable_commits, 1);
        }
    }
}

impl Drop for PhaseTimer<'_> {
    fn drop(&mut self) {
        self.state.record_elapsed(
            self.phase,
            self.elapsed.unwrap_or_else(|| self.start.elapsed()),
        );
        match self.phase {
            CommitPhase::Commit if self.success => add(&self.state.physical_commits, 1),
            CommitPhase::Commit => add(&self.state.commit_errors, 1),
            CommitPhase::Write if !self.success => add(&self.state.write_errors, 1),
            CommitPhase::Sync if !self.success => add(&self.state.sync_errors, 1),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AtomicPhaseMetrics, CommitMetrics, CommitPhase, MetricsState};

    #[test]
    fn disabled_collection_records_nothing() {
        let metrics = MetricsState::default();
        assert!(metrics.start(CommitPhase::Commit).is_none());
        metrics.record_group(5);
        metrics.record_conflict();
        metrics.record_queue_rejection();
        assert_eq!(metrics.snapshot(), CommitMetrics::default());
    }

    #[test]
    fn attempts_errors_and_successful_bytes_are_distinct() {
        let metrics = MetricsState::default();
        metrics.set_enabled(true);
        metrics
            .start(CommitPhase::Write)
            .unwrap()
            .complete_write(true, 128);
        metrics
            .start(CommitPhase::Write)
            .unwrap()
            .complete_write(false, 256);
        metrics.start(CommitPhase::Sync).unwrap().complete(true);
        drop(metrics.start(CommitPhase::Sync));
        metrics
            .start(CommitPhase::Commit)
            .unwrap()
            .complete_commit(true, true);
        metrics
            .start(CommitPhase::Commit)
            .unwrap()
            .complete_commit(true, false);
        drop(metrics.start(CommitPhase::Commit));
        metrics.record_group(3);
        metrics.record_conflict();
        metrics.record_queue_rejection();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.write.calls, 2);
        assert_eq!(snapshot.written_bytes, 128);
        assert_eq!(snapshot.write_errors, 1);
        assert_eq!(snapshot.sync.calls, 2);
        assert_eq!(snapshot.sync_errors, 1);
        assert_eq!(snapshot.commit.calls, 3);
        assert_eq!(snapshot.physical_commits, 2);
        assert_eq!(snapshot.durable_commits, 1);
        assert_eq!(snapshot.commit_errors, 1);
        assert_eq!(snapshot.group_batches, 1);
        assert_eq!(snapshot.grouped_transactions, 3);
        assert_eq!(snapshot.max_group_size, 3);
        assert_eq!(snapshot.conflicts, 1);
        assert_eq!(snapshot.queue_rejections, 1);
        metrics.set_enabled(false);
        assert_eq!(metrics.snapshot().written_bytes, 128);
        assert!(metrics.start(CommitPhase::Write).is_none());
    }

    #[test]
    fn cumulative_values_saturate_and_maximum_is_not_additive() {
        let metrics = AtomicPhaseMetrics::default();
        metrics.record(10);
        metrics.record(20);
        assert_eq!(metrics.snapshot().total_ns, 30);
        assert_eq!(metrics.snapshot().max_ns, 20);
        metrics.record(u64::MAX);
        metrics.record(1);
        assert_eq!(metrics.snapshot().total_ns, u64::MAX);
        assert_eq!(metrics.snapshot().max_ns, u64::MAX);
    }
}
