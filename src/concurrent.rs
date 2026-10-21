//! Optimistic write transactions with private, owned changes.

#[cfg(not(redb_no_std))]
use crate::commit_metrics::CommitPhase;
use crate::transaction_tracker::TransactionId;
#[cfg(not(feature = "experimental-api-5"))]
use crate::tree_store::encode_bounds;
use crate::tree_store::{InternalTableDefinition, MAX_PAIR_LENGTH, MAX_VALUE_LENGTH};
use crate::{
    CommitError, Database, Durability, Key, ReadOnlyTable, ReadTransaction, ReadableDatabase,
    ReadableTableMetadata, SetDurabilityError, StorageError, TableDefinition, TableError,
    TableHandle, TransactionError, TypeName, Value, WriteTransaction,
};
#[cfg(feature = "experimental-api-5")]
use crate::{KeyRange, ReadableTable};
use alloc::collections::btree_map::Entry;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::cmp::Ordering;
use core::fmt::{self, Debug, Display, Formatter};
use core::iter::FusedIterator;
use core::marker::PhantomData;
use core::mem::size_of;
use core::ops::Bound;
#[cfg(not(feature = "experimental-api-5"))]
use core::ops::RangeBounds;

/// An error committing an optimistic write transaction.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConcurrentWriteError {
    /// A value read or written by the transaction changed, or a scan observed a changed table.
    /// No changes from this transaction were committed. Retry with a fresh transaction.
    Conflict { table: String },
    /// Error reading or applying buffered changes.
    Storage(StorageError),
    /// Error opening a table.
    Table(TableError),
    /// Error beginning the serialized commit transaction.
    Transaction(TransactionError),
    /// Error publishing the transaction. The changes may already be durable, as for
    /// [`WriteTransaction::commit`]; do not retry this error as a conflict.
    Commit(CommitError),
    /// The selected durability cannot be used for this database.
    Durability(SetDurabilityError),
    /// The configured group-commit queue is full. This transaction made no storage changes.
    #[cfg(not(redb_no_std))]
    QueueFull,
    /// A shared batch failed. Inspect the cause; a commit I/O error may already be durable.
    /// This error must not be retried automatically as a conflict.
    #[cfg(not(redb_no_std))]
    BatchAborted(alloc::sync::Arc<ConcurrentWriteError>),
    /// The batch leader panicked. Its publication may have completed; do not retry as a conflict.
    #[cfg(not(redb_no_std))]
    BatchPanicked,
}

impl Display for ConcurrentWriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { table } => write!(f, "Concurrent write conflict in table '{table}'"),
            Self::Storage(error) => Display::fmt(error, f),
            Self::Table(error) => Display::fmt(error, f),
            Self::Transaction(error) => Display::fmt(error, f),
            Self::Commit(error) => Display::fmt(error, f),
            Self::Durability(error) => Display::fmt(error, f),
            #[cfg(not(redb_no_std))]
            Self::QueueFull => f.write_str("The group-commit queue is full"),
            #[cfg(not(redb_no_std))]
            Self::BatchAborted(error) => write!(f, "Group commit failed: {error}"),
            #[cfg(not(redb_no_std))]
            Self::BatchPanicked => {
                f.write_str("The group-commit leader panicked; publication is uncertain")
            }
        }
    }
}

impl core::error::Error for ConcurrentWriteError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Table(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::Commit(error) => Some(error),
            Self::Durability(error) => Some(error),
            #[cfg(not(redb_no_std))]
            Self::BatchAborted(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

macro_rules! from_error {
    ($source:ty, $variant:ident) => {
        impl From<$source> for ConcurrentWriteError {
            fn from(error: $source) -> Self {
                Self::$variant(error)
            }
        }
    };
}

from_error!(StorageError, Storage);
from_error!(TableError, Table);
from_error!(TransactionError, Transaction);
from_error!(CommitError, Commit);
from_error!(SetDurabilityError, Durability);

/// An owned value returned by a concurrent write transaction.
///
/// Unlike a page-backed access guard, this value remains usable after its transaction ends.
pub struct ConcurrentValue<V: Value + 'static> {
    bytes: Vec<u8>,
    marker: PhantomData<fn() -> V>,
}

impl<V: Value + 'static> ConcurrentValue<V> {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            marker: PhantomData,
        }
    }

    pub fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.bytes)
    }
}

impl<V: Value + 'static> Clone for ConcurrentValue<V> {
    fn clone(&self) -> Self {
        Self::new(self.bytes.clone())
    }
}

impl<V: Value + 'static> Debug for ConcurrentValue<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.value().fmt(f)
    }
}

struct EncodedKey {
    bytes: Vec<u8>,
    compare: fn(&[u8], &[u8]) -> Ordering,
}

impl PartialEq for EncodedKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for EncodedKey {}

impl PartialOrd for EncodedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EncodedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.compare)(&self.bytes, &other.bytes)
    }
}

struct ObservedValue {
    original: Option<Vec<u8>>,
    change: Change,
}

enum Change {
    Read,
    Delete,
    Insert {
        // Updates retain the stored encoding. Inserting an absent or explicitly removed
        // key uses this encoding, even when the comparator equates different encodings.
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    },
}

impl ObservedValue {
    fn visible(&self) -> Option<&[u8]> {
        match &self.change {
            Change::Read => self.original.as_deref(),
            Change::Delete => None,
            Change::Insert { value, .. } => Some(value),
        }
    }
}

type Validate = fn(
    &ReadTransaction,
    &str,
    &TableState,
    Option<&InternalTableDefinition>,
    bool,
) -> Result<(), ConcurrentWriteError>;
type Apply = fn(&WriteTransaction, &str, &TableState) -> Result<(), ConcurrentWriteError>;

struct TableState {
    definition: Option<InternalTableDefinition>,
    key_type: TypeName,
    value_type: TypeName,
    key_width: Option<usize>,
    value_width: Option<usize>,
    compare: fn(&[u8], &[u8]) -> Ordering,
    observations: BTreeMap<EncodedKey, ObservedValue>,
    whole_table_read: bool,
    validate: Validate,
    apply: Apply,
}

impl TableState {
    fn new<K: Key + 'static, V: Value + 'static>(
        definition: Option<InternalTableDefinition>,
    ) -> Self {
        Self {
            definition,
            key_type: K::type_name(),
            value_type: V::type_name(),
            key_width: K::fixed_width(),
            value_width: V::fixed_width(),
            compare: K::compare,
            observations: BTreeMap::new(),
            whole_table_read: false,
            validate: validate::<K, V>,
            apply: apply::<K, V>,
        }
    }

    fn has_writes(&self) -> bool {
        self.definition.is_none()
            || self
                .observations
                .values()
                .any(|value| !matches!(value.change, Change::Read))
    }

    fn check_types<K: Key, V: Value>(&self, name: &str) -> Result<(), TableError> {
        if self.key_type != K::type_name() || self.value_type != V::type_name() {
            return Err(TableError::TableTypeMismatch {
                table: name.to_string(),
                key: self.key_type.clone(),
                value: self.value_type.clone(),
            });
        }
        for (name, expected, actual) in [
            (self.key_type.clone(), self.key_width, K::fixed_width()),
            (self.value_type.clone(), self.value_width, V::fixed_width()),
        ] {
            if expected != actual {
                return Err(TableError::TypeDefinitionChanged {
                    name,
                    alignment: 1,
                    width: expected,
                });
            }
        }
        Ok(())
    }
}

fn schema(definition: &InternalTableDefinition) -> InternalTableDefinition {
    let mut result = definition.clone();
    match &mut result {
        InternalTableDefinition::Normal {
            table_root,
            table_length,
            ..
        }
        | InternalTableDefinition::Multimap {
            table_root,
            table_length,
            ..
        } => {
            *table_root = None;
            *table_length = 0;
        }
    }
    result
}

fn conflict(table: &str) -> ConcurrentWriteError {
    ConcurrentWriteError::Conflict {
        table: table.to_string(),
    }
}

fn validate<K: Key + 'static, V: Value + 'static>(
    latest: &ReadTransaction,
    name: &str,
    state: &TableState,
    current: Option<&InternalTableDefinition>,
    unchanged_keys: bool,
) -> Result<(), ConcurrentWriteError> {
    if current == state.definition.as_ref() {
        return Ok(());
    }
    let (Some(current), Some(original)) = (current, &state.definition) else {
        return Err(conflict(name));
    };
    if state.whole_table_read || schema(current) != schema(original) {
        return Err(conflict(name));
    }
    if unchanged_keys {
        return Ok(());
    }
    let table = latest.open_table(TableDefinition::<K, V>::new(name))?;
    for (key, observed) in &state.observations {
        let value = table.get(K::from_bytes(&key.bytes))?;
        let matches = match (value, observed.original.as_deref()) {
            (Some(guard), Some(original)) => V::as_bytes(&guard.value()).as_ref() == original,
            (None, None) => true,
            _ => false,
        };
        if !matches {
            return Err(conflict(name));
        }
    }
    Ok(())
}

fn apply<K: Key + 'static, V: Value + 'static>(
    transaction: &WriteTransaction,
    name: &str,
    state: &TableState,
) -> Result<(), ConcurrentWriteError> {
    if !state.has_writes() {
        return Ok(());
    }
    let mut table = transaction.open_table(TableDefinition::<K, V>::new(name))?;
    for (key, observed) in &state.observations {
        match &observed.change {
            Change::Insert {
                key: inserted,
                value,
            } => {
                if inserted.is_some() && observed.original.is_some() {
                    table.remove(K::from_bytes(&key.bytes))?;
                }
                let key = inserted.as_deref().unwrap_or(&key.bytes);
                table.insert(K::from_bytes(key), V::from_bytes(value))?;
            }
            Change::Delete => {
                table.remove(K::from_bytes(&key.bytes))?;
            }
            Change::Read => {}
        }
    }
    Ok(())
}

const HISTORY_COMMITS: usize = 64;
const HISTORY_BYTES: usize = 64 * 1024;

struct WrittenTable {
    name: String,
    keys: Vec<Vec<u8>>,
}

struct Footprint {
    tables: Vec<WrittenTable>,
    bytes: usize,
}

impl Footprint {
    // Check the entire footprint before allocating its copies. Large transactions simply
    // forgo the cache; they never trigger an unbounded history allocation or eviction pass.
    fn new<'a>(tables: impl Iterator<Item = (&'a String, &'a TableState)> + Clone) -> Option<Self> {
        let mut bytes = size_of::<CommitRecord>();
        let mut written_tables = 0;
        for (name, state) in tables.clone() {
            let mut any_written = false;
            for (key, observed) in &state.observations {
                if matches!(observed.change, Change::Read) {
                    continue;
                }
                if !any_written {
                    bytes = bytes
                        .saturating_add(size_of::<WrittenTable>())
                        .saturating_add(name.len());
                    written_tables += 1;
                    any_written = true;
                }
                bytes = bytes
                    .saturating_add(size_of::<Vec<u8>>())
                    .saturating_add(key.bytes.len());
                if bytes > HISTORY_BYTES {
                    return None;
                }
            }
        }
        let mut result = Vec::with_capacity(written_tables);
        for (name, state) in tables {
            let count = state
                .observations
                .values()
                .filter(|observed| !matches!(observed.change, Change::Read))
                .count();
            if count == 0 {
                continue;
            }
            let mut keys = Vec::with_capacity(count);
            for (key, observed) in &state.observations {
                if !matches!(observed.change, Change::Read) {
                    keys.push(key.bytes.clone());
                }
            }
            result.push(WrittenTable {
                name: name.clone(),
                keys,
            });
        }
        Some(Self {
            tables: result,
            bytes,
        })
    }
}

struct CommitRecord {
    previous: TransactionId,
    committed: TransactionId,
    footprint: Footprint,
}

/// An optional validation cache. A missing record is never evidence of an unchanged key.
#[derive(Default)]
pub(crate) struct CommitHistory {
    records: VecDeque<CommitRecord>,
    bytes: usize,
}

impl CommitHistory {
    fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }

    fn covered(
        &self,
        snapshot: TransactionId,
        latest: TransactionId,
    ) -> Option<core::ops::Range<usize>> {
        if snapshot == latest {
            return Some(0..0);
        }
        let start = self
            .records
            .iter()
            .position(|record| record.previous == snapshot)?;
        let mut previous = snapshot;
        for (index, record) in self.records.iter().enumerate().skip(start) {
            if record.previous != previous || record.committed <= previous {
                return None;
            }
            previous = record.committed;
            if previous == latest {
                return Some(start..index + 1);
            }
            if previous > latest {
                return None;
            }
        }
        None
    }

    fn keys_unchanged(
        &self,
        covered: core::ops::Range<usize>,
        name: &str,
        state: &TableState,
    ) -> bool {
        let Some((first, last)) = state
            .observations
            .first_key_value()
            .zip(state.observations.last_key_value())
        else {
            return true;
        };
        for record in self.records.range(covered) {
            for table in &record.footprint.tables {
                if table.name != name
                    || (state.compare)(table.keys.last().unwrap(), &first.0.bytes).is_lt()
                    || (state.compare)(table.keys.first().unwrap(), &last.0.bytes).is_gt()
                {
                    continue;
                }
                if state.observations.len() > table.keys.len().saturating_mul(8) {
                    for key in &table.keys {
                        if state.observations.contains_key(&EncodedKey {
                            bytes: key.clone(),
                            compare: state.compare,
                        }) {
                            return false;
                        }
                    }
                } else {
                    let mut observations = state.observations.keys().peekable();
                    for key in &table.keys {
                        while let Some(observed) = observations.peek() {
                            match (state.compare)(&observed.bytes, key) {
                                Ordering::Less => {
                                    observations.next();
                                }
                                Ordering::Equal => return false,
                                Ordering::Greater => break,
                            }
                        }
                    }
                }
            }
        }
        true
    }

    fn record(
        &mut self,
        previous: TransactionId,
        committed: TransactionId,
        footprint: Option<Footprint>,
    ) {
        let Some(footprint) = footprint else {
            self.clear();
            return;
        };
        if committed <= previous {
            self.clear();
            return;
        }
        if self
            .records
            .back()
            .is_some_and(|record| record.committed != previous)
        {
            self.clear();
        }
        while self.records.len() >= HISTORY_COMMITS || self.bytes + footprint.bytes > HISTORY_BYTES
        {
            let oldest = self.records.pop_front().unwrap();
            self.bytes -= oldest.footprint.bytes;
        }
        self.bytes += footprint.bytes;
        self.records.push_back(CommitRecord {
            previous,
            committed,
            footprint,
        });
    }
}

/// A write transaction whose reads and private changes can be prepared alongside other writers.
///
/// Preparation takes no writer lock. Commit takes the ordinary writer lock to validate
/// the transaction's snapshot, apply its buffered changes, and publish one atomic commit. A
/// changed point-read or written key returns [`ConcurrentWriteError::Conflict`]. Changes to
/// unrelated keys can both commit without retrying. `len`, `is_empty`, and range reads conservatively
/// conflict with any change to their table, including phantom insertions.
///
/// Values and read dependencies are owned in memory, proportional to the keys and values touched.
/// The transaction borrows its database and supports normal tables. Savepoints, catalog changes
/// other than creating a table, and multimap tables use [`WriteTransaction`] instead. A conflict
/// requires a fresh transaction; user code is never automatically retried.
pub struct ConcurrentWriteTransaction<'db> {
    database: &'db Database,
    snapshot: ReadTransaction,
    tables: BTreeMap<String, TableState>,
    catalog_reads: BTreeMap<String, Option<InternalTableDefinition>>,
    durability: Durability,
}

impl<'db> ConcurrentWriteTransaction<'db> {
    pub(crate) fn new(database: &'db Database) -> Result<Self, TransactionError> {
        Ok(Self {
            database,
            snapshot: database.begin_read()?,
            tables: BTreeMap::new(),
            catalog_reads: BTreeMap::new(),
            durability: Durability::Immediate,
        })
    }

    /// Open a normal table, creating it at commit if it did not exist in the snapshot.
    ///
    /// Close this scoped handle before opening another table or committing. Reopening a table
    /// retains its staged changes and all of its original read dependencies.
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &'txn mut self,
        definition: TableDefinition<K, V>,
    ) -> Result<ConcurrentTable<'txn, K, V>, TableError> {
        let name = definition.name();
        let stored = self.snapshot.table_definition(name)?;
        // A handled type/kind error is still an observation used by application decisions.
        self.catalog_reads
            .entry(name.to_string())
            .or_insert_with(|| stored.as_ref().map(schema));
        let base = if stored.is_some() {
            Some(self.snapshot.open_table(definition)?)
        } else {
            None
        };
        let state = self
            .tables
            .entry(name.to_string())
            .or_insert_with(|| TableState::new::<K, V>(stored));
        state.check_types::<K, V>(name)?;
        Ok(ConcurrentTable { state, base })
    }

    /// Select commit durability. Defaults to [`Durability::Immediate`].
    pub fn set_durability(&mut self, durability: Durability) -> Result<(), SetDurabilityError> {
        #[cfg(feature = "experimental-multiprocess")]
        if matches!(durability, Durability::None)
            && self
                .database
                .get_memory()
                .concurrency_mode()
                .is_multi_process_writable()
        {
            return Err(SetDurabilityError::NonDurableCommitUnsupported);
        }
        self.durability = durability;
        Ok(())
    }

    /// Validate all dependencies and commit all buffered changes atomically.
    ///
    /// A conflict commits none of this transaction's changes. Other commit errors have the
    /// same durability and recovery contract as [`WriteTransaction::commit`].
    pub fn commit(self) -> Result<(), ConcurrentWriteError> {
        let Self {
            database,
            snapshot,
            tables,
            catalog_reads,
            durability,
        } = self;
        let prepared = PreparedWrite {
            snapshot,
            tables,
            catalog_reads,
            durability,
        };
        #[cfg(not(redb_no_std))]
        if matches!(durability, Durability::Immediate)
            && let Some(coordinator) = &database.group_commit
        {
            return coordinator.commit(database, prepared);
        }
        let result = prepared.commit_alone(database);
        #[cfg(not(redb_no_std))]
        if matches!(&result, Err(ConcurrentWriteError::Conflict { .. })) {
            database.get_memory().metrics_state().record_conflict();
        }
        result
    }

    /// Discard the private changes. Dropping the transaction has the same effect.
    pub fn abort(self) {}
}

/// Owned request data; unlike the public handle this has no borrowed database reference.
pub(crate) struct PreparedWrite {
    snapshot: ReadTransaction,
    tables: BTreeMap<String, TableState>,
    catalog_reads: BTreeMap<String, Option<InternalTableDefinition>>,
    durability: Durability,
}

impl PreparedWrite {
    pub(crate) fn commit_alone(self, database: &Database) -> Result<(), ConcurrentWriteError> {
        let footprint = Footprint::new(self.tables.iter());
        #[cfg(not(redb_no_std))]
        let memory = database.get_memory();
        let mut writer = database.begin_write()?;
        writer.set_durability(self.durability)?;
        // Take the writer slot before history: a waiting writer must never hold history
        // while the committing writer needs it to finish publication.
        let mut history = match database.concurrent_history.lock() {
            Ok(history) => history,
            Err(poisoned) => {
                let mut history = poisoned.into_inner();
                history.clear();
                #[cfg(not(redb_no_std))]
                database.concurrent_history.clear_poison();
                history
            }
        };
        let latest = database.begin_read().inspect_err(|_| history.clear())?;
        #[cfg(not(redb_no_std))]
        let validation = memory.metrics_state().start(CommitPhase::Validation);
        let previous = latest.snapshot_id();
        let covered = history.covered(self.snapshot.snapshot_id(), previous);
        for (name, expected) in &self.catalog_reads {
            let current = latest
                .table_definition(name)
                .inspect_err(|_| history.clear())?;
            if current.as_ref().map(schema) != *expected {
                return Err(conflict(name));
            }
            if let Some(state) = self.tables.get(name) {
                let unchanged_keys = !state.whole_table_read
                    && current.as_ref() != state.definition.as_ref()
                    && covered
                        .as_ref()
                        .is_some_and(|range| history.keys_unchanged(range.clone(), name, state));
                (state.validate)(&latest, name, state, current.as_ref(), unchanged_keys)?;
            }
        }
        #[cfg(not(redb_no_std))]
        drop(validation);
        drop(latest);
        if !self.tables.values().any(TableState::has_writes) {
            writer.abort().inspect_err(|_| history.clear())?;
            return Ok(());
        }
        #[cfg(not(redb_no_std))]
        let replay = memory.metrics_state().start(CommitPhase::Replay);
        for (name, state) in &self.tables {
            (state.apply)(&writer, name, state).inspect_err(|_| history.clear())?;
        }
        #[cfg(not(redb_no_std))]
        drop(replay);
        drop(self.snapshot);
        let (committed, writer) = match writer.commit_and_hold() {
            Ok(result) => result,
            Err(error) => {
                history.clear();
                return Err(error.into());
            }
        };
        history.record(previous, committed, footprint);
        // Publish and release history before waking the next writer. Otherwise the next
        // writer can take its slot only to park again waiting for history publication.
        drop(history);
        drop(writer);
        Ok(())
    }
}

#[cfg(not(redb_no_std))]
pub(crate) struct BatchOutcome {
    pub(crate) results: Vec<Result<(), ConcurrentWriteError>>,
    pub(crate) published: bool,
}

#[cfg(not(redb_no_std))]
impl PreparedWrite {
    // Account all owned buffers, including read preimages, plus a conservative allowance
    // for BTreeMap node slots. Shared cache pages and allocator/RSS overhead are separate.
    pub(crate) fn accounted_bytes(&self) -> usize {
        fn node_bytes<K, V>() -> usize {
            size_of::<(K, V)>().saturating_mul(12).saturating_add(128)
        }
        fn definition_bytes(definition: Option<&InternalTableDefinition>) -> usize {
            match definition {
                Some(
                    InternalTableDefinition::Normal {
                        key_type,
                        value_type,
                        ..
                    }
                    | InternalTableDefinition::Multimap {
                        key_type,
                        value_type,
                        ..
                    },
                ) => key_type
                    .name()
                    .len()
                    .saturating_add(value_type.name().len()),
                None => 0,
            }
        }
        let mut bytes = size_of::<Self>().saturating_add(256);
        for (name, state) in &self.tables {
            bytes = bytes
                .saturating_add(node_bytes::<String, TableState>())
                .saturating_add(name.capacity())
                .saturating_add(state.key_type.name().len())
                .saturating_add(state.value_type.name().len())
                .saturating_add(definition_bytes(state.definition.as_ref()));
            for (key, observed) in &state.observations {
                bytes = bytes
                    .saturating_add(node_bytes::<EncodedKey, ObservedValue>())
                    .saturating_add(key.bytes.capacity())
                    .saturating_add(observed.original.as_ref().map_or(0, Vec::capacity));
                if let Change::Insert { key, value } = &observed.change {
                    bytes = bytes
                        .saturating_add(key.as_ref().map_or(0, Vec::capacity))
                        .saturating_add(value.capacity());
                }
            }
        }
        for (name, definition) in &self.catalog_reads {
            bytes = bytes
                .saturating_add(node_bytes::<String, Option<InternalTableDefinition>>())
                .saturating_add(name.capacity())
                .saturating_add(definition_bytes(definition.as_ref()));
        }
        bytes
    }

    fn validate_for_batch(
        &self,
        latest: &ReadTransaction,
        history: &CommitHistory,
    ) -> Result<(), ConcurrentWriteError> {
        let covered = history.covered(self.snapshot.snapshot_id(), latest.snapshot_id());
        for (name, expected) in &self.catalog_reads {
            let current = latest.table_definition(name)?;
            if current.as_ref().map(schema) != *expected {
                return Err(conflict(name));
            }
            if let Some(state) = self.tables.get(name) {
                let unchanged_keys = !state.whole_table_read
                    && current.as_ref() != state.definition.as_ref()
                    && covered
                        .as_ref()
                        .is_some_and(|range| history.keys_unchanged(range.clone(), name, state));
                (state.validate)(latest, name, state, current.as_ref(), unchanged_keys)?;
            }
        }
        Ok(())
    }

    // The batch serializes in queue order. Earlier reads may precede later writes;
    // earlier writes must not invalidate any later point, scan or catalog observation.
    fn validate_after(&self, earlier: &Self) -> Result<(), ConcurrentWriteError> {
        for (name, state) in &earlier.tables {
            if !state.has_writes() || !self.catalog_reads.contains_key(name) {
                continue;
            }
            if state.definition.is_none() {
                return Err(conflict(name));
            }
            let Some(observed) = self.tables.get(name) else {
                continue;
            };
            if observed.whole_table_read {
                return Err(conflict(name));
            }
            for (key, value) in &state.observations {
                if matches!(value.change, Change::Read) {
                    continue;
                }
                if observed.observations.contains_key(&EncodedKey {
                    bytes: key.bytes.clone(),
                    compare: observed.compare,
                }) {
                    return Err(conflict(name));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn commit_batch(database: &Database, mut requests: Vec<Self>) -> BatchOutcome {
        if requests.len() == 1 {
            let request = requests.pop().unwrap();
            let writes = request.tables.values().any(TableState::has_writes);
            let result = request.commit_alone(database);
            if matches!(&result, Err(ConcurrentWriteError::Conflict { .. })) {
                database.get_memory().metrics_state().record_conflict();
            }
            let published = writes && result.is_ok();
            return BatchOutcome {
                results: alloc::vec![result],
                published,
            };
        }
        let mut outcomes: Vec<Option<Result<(), ConcurrentWriteError>>> =
            (0..requests.len()).map(|_| None).collect();
        let failed = |outcomes: Vec<Option<Result<(), ConcurrentWriteError>>>,
                      error: ConcurrentWriteError| {
            let cause = alloc::sync::Arc::new(error);
            BatchOutcome {
                results: outcomes
                    .into_iter()
                    .map(|result| {
                        result.unwrap_or_else(|| {
                            Err(ConcurrentWriteError::BatchAborted(cause.clone()))
                        })
                    })
                    .collect(),
                published: false,
            }
        };
        let memory = database.get_memory();
        let mut writer = match database.begin_write() {
            Ok(writer) => writer,
            Err(error) => return failed(outcomes, error.into()),
        };
        if let Err(error) = writer.set_durability(Durability::Immediate) {
            return failed(outcomes, error.into());
        }
        let mut history = match database.concurrent_history.lock() {
            Ok(history) => history,
            Err(poisoned) => {
                let mut history = poisoned.into_inner();
                history.clear();
                database.concurrent_history.clear_poison();
                history
            }
        };
        let latest = match database.begin_read() {
            Ok(latest) => latest,
            Err(error) => {
                history.clear();
                return failed(outcomes, error.into());
            }
        };
        let validation = memory.metrics_state().start(CommitPhase::Validation);
        let mut accepted = Vec::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            let result = request
                .validate_for_batch(&latest, &history)
                .and_then(|()| {
                    for &earlier in &accepted {
                        request.validate_after(&requests[earlier])?;
                    }
                    Ok(())
                });
            match result {
                Ok(()) => accepted.push(index),
                Err(error @ ConcurrentWriteError::Conflict { .. }) => {
                    memory.metrics_state().record_conflict();
                    outcomes[index] = Some(Err(error));
                }
                Err(error) => {
                    history.clear();
                    return failed(outcomes, error);
                }
            }
        }
        drop(validation);
        let previous = latest.snapshot_id();
        drop(latest);
        let footprint = Footprint::new(
            accepted
                .iter()
                .flat_map(|&index| requests[index].tables.iter()),
        );
        let writes = accepted
            .iter()
            .any(|&index| requests[index].tables.values().any(TableState::has_writes));
        if !writes {
            if let Err(error) = writer.abort() {
                history.clear();
                return failed(outcomes, error.into());
            }
        } else {
            let replay = memory.metrics_state().start(CommitPhase::Replay);
            for &index in &accepted {
                for (name, state) in &requests[index].tables {
                    if let Err(error) = (state.apply)(&writer, name, state) {
                        history.clear();
                        return failed(outcomes, error);
                    }
                }
            }
            drop(replay);
            drop(requests);
            let (committed, committed_writer) = match writer.commit_and_hold() {
                Ok(result) => result,
                Err(error) => {
                    history.clear();
                    return failed(outcomes, error.into());
                }
            };
            // Only accepted writes enter this one physical epoch's bounded footprint.
            history.record(previous, committed, footprint);
            drop(history);
            drop(committed_writer);
        }
        for index in accepted {
            outcomes[index] = Some(Ok(()));
        }
        BatchOutcome {
            results: outcomes.into_iter().map(Option::unwrap).collect(),
            published: writes,
        }
    }
}

/// A scoped, typed view of a concurrent transaction's private table changes.
pub struct ConcurrentTable<'txn, K: Key + 'static, V: Value + 'static> {
    state: &'txn mut TableState,
    base: Option<ReadOnlyTable<K, V>>,
}

impl<K: Key + 'static, V: Value + 'static> ConcurrentTable<'_, K, V> {
    fn key(&self, key: &K::SelfType<'_>) -> EncodedKey {
        EncodedKey {
            bytes: K::as_bytes(key).as_ref().to_vec(),
            compare: self.state.compare,
        }
    }

    fn observe(&mut self, key: EncodedKey) -> crate::Result<&mut ObservedValue> {
        match self.state.observations.entry(key) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let original = if let Some(table) = &self.base {
                    table
                        .get(K::from_bytes(&entry.key().bytes))?
                        .map(|guard| V::as_bytes(&guard.value()).as_ref().to_vec())
                } else {
                    None
                };
                Ok(entry.insert(ObservedValue {
                    original,
                    change: Change::Read,
                }))
            }
        }
    }

    /// Read this transaction's staged value, falling back to its starting snapshot.
    pub fn get<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<ConcurrentValue<V>>> {
        let key = self.key(key.borrow());
        let observed = self.observe(key)?;
        Ok(observed
            .visible()
            .map(|bytes| ConcurrentValue::new(bytes.to_vec())))
    }

    /// Stage an insertion, returning the previous value visible to this transaction.
    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> crate::Result<Option<ConcurrentValue<V>>> {
        let key_bytes = K::as_bytes(key.borrow());
        let value_bytes = V::as_bytes(value.borrow());
        let key_len = key_bytes.as_ref().len();
        let value_len = value_bytes.as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let pair_len = key_len.saturating_add(value_len);
        if pair_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(pair_len));
        }
        let bytes = value_bytes.as_ref().to_vec();
        let key = EncodedKey {
            bytes: key_bytes.as_ref().to_vec(),
            compare: self.state.compare,
        };
        let inserted_key = key.bytes.clone();
        let observed = self.observe(key)?;
        let old = observed.visible().map(<[u8]>::to_vec);
        let inserted_key = if old.is_none() {
            Some(inserted_key)
        } else if let Change::Insert { key, .. } = &observed.change {
            key.clone()
        } else {
            None
        };
        observed.change = Change::Insert {
            key: inserted_key,
            value: bytes,
        };
        Ok(old.map(ConcurrentValue::new))
    }

    /// Stage a removal, returning the previous value visible to this transaction.
    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<ConcurrentValue<V>>> {
        let key = self.key(key.borrow());
        let observed = self.observe(key)?;
        let old = observed.visible().map(<[u8]>::to_vec);
        observed.change = Change::Delete;
        Ok(old.map(ConcurrentValue::new))
    }

    /// Count entries visible to this transaction. Any concurrent table change conflicts.
    pub fn len(&mut self) -> crate::Result<u64> {
        self.state.whole_table_read = true;
        let mut count = self
            .base
            .as_ref()
            .map_or(Ok(0), ReadableTableMetadata::len)?;
        for observed in self.state.observations.values() {
            if !matches!(observed.change, Change::Read) {
                match (observed.original.is_some(), observed.visible().is_some()) {
                    (false, true) => count += 1,
                    (true, false) => count -= 1,
                    _ => {}
                }
            }
        }
        Ok(count)
    }

    pub fn is_empty(&mut self) -> crate::Result<bool> {
        self.len().map(|len| len == 0)
    }

    /// Read an owned snapshot of a range, including staged changes.
    ///
    /// The selected range is materialized in memory. Any concurrent change to the table
    /// conflicts at commit, including changes outside the selected bounds.
    #[cfg(feature = "experimental-api-5")]
    pub fn range(&mut self, range: impl KeyRange<K>) -> crate::Result<ConcurrentRange<K, V>> {
        let (lower, upper) = range.key_bounds();
        self.range_in_bounds(lower, upper)
    }

    /// Read an owned snapshot of a range, including staged changes.
    ///
    /// The selected range is materialized in memory. Any concurrent change to the table
    /// conflicts at commit, including changes outside the selected bounds.
    #[cfg(not(feature = "experimental-api-5"))]
    pub fn range<'a, KR>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
    ) -> crate::Result<ConcurrentRange<K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let (lower, upper) = encode_bounds::<K, KR, _>(&range);
        self.range_in_bounds(lower, upper)
    }

    /// Read an owned snapshot of all entries, including staged changes.
    pub fn iter(&mut self) -> crate::Result<ConcurrentRange<K, V>> {
        self.range_in_bounds(Bound::Unbounded, Bound::Unbounded)
    }

    fn range_in_bounds(
        &mut self,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> crate::Result<ConcurrentRange<K, V>> {
        self.state.whole_table_read = true;
        let mut entries = BTreeMap::new();
        if let Some(table) = &self.base {
            let start = lower.as_ref().map(|bytes| K::from_bytes(bytes));
            let end = upper.as_ref().map(|bytes| K::from_bytes(bytes));
            for entry in table.range((start, end))? {
                let (key, value) = entry?;
                entries.insert(
                    self.key(&key.value()),
                    V::as_bytes(&value.value()).as_ref().to_vec(),
                );
            }
        }
        for (key, observed) in &self.state.observations {
            if !in_bounds::<K>(&key.bytes, &lower, &upper) {
                continue;
            }
            match &observed.change {
                Change::Insert {
                    key: inserted,
                    value,
                } => {
                    if inserted.is_some() {
                        entries.remove(key);
                    }
                    entries.insert(
                        EncodedKey {
                            bytes: inserted.as_ref().unwrap_or(&key.bytes).clone(),
                            compare: key.compare,
                        },
                        value.clone(),
                    );
                }
                Change::Delete => {
                    entries.remove(key);
                }
                Change::Read => {}
            }
        }
        Ok(ConcurrentRange {
            inner: entries
                .into_iter()
                .map(|(key, value)| (ConcurrentValue::new(key.bytes), ConcurrentValue::new(value)))
                .collect::<Vec<_>>()
                .into_iter(),
        })
    }
}

fn in_bounds<K: Key>(key: &[u8], lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    let above = match lower {
        Bound::Included(bound) => K::compare(key, bound).is_ge(),
        Bound::Excluded(bound) => K::compare(key, bound).is_gt(),
        Bound::Unbounded => true,
    };
    let below = match upper {
        Bound::Included(bound) => K::compare(key, bound).is_le(),
        Bound::Excluded(bound) => K::compare(key, bound).is_lt(),
        Bound::Unbounded => true,
    };
    above && below
}

/// An owned range from a concurrent table, in its key type's ordering.
pub struct ConcurrentRange<K: Key + 'static, V: Value + 'static> {
    inner: alloc::vec::IntoIter<(ConcurrentValue<K>, ConcurrentValue<V>)>,
}

impl<K: Key + 'static, V: Value + 'static> Iterator for ConcurrentRange<K, V> {
    type Item = crate::Result<(ConcurrentValue<K>, ConcurrentValue<V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K: Key + 'static, V: Value + 'static> DoubleEndedIterator for ConcurrentRange<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(Ok)
    }
}

impl<K: Key + 'static, V: Value + 'static> ExactSizeIterator for ConcurrentRange<K, V> {}
impl<K: Key + 'static, V: Value + 'static> FusedIterator for ConcurrentRange<K, V> {}
