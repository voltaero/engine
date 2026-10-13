use std::sync::Arc;

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use rust_rocksdb::{Direction, IteratorMode, Range, ReadOptions, WriteBatch, properties};
use serde::{Deserialize, Serialize};

use crate::Identifier;
use crate::api::{
    chunk_seq_from_key, finished_task_key, finished_task_key_prefix, lock_lease, task_key,
    task_key_prefix,
};
use crate::chunk::{Bitmap, ChunkPayloads, FinishedChunk};
use crate::wire::{PageRecord, Scope, TaskRecord, TaskStatus};
use crate::{api::ServerAPI, error::Error};

/// Administrative / export operations over the task store.
///
/// This is the operator-facing surface — inspect, back up, and prune records —
/// distinct from the task lifecycle (submit / lease / complete). Every variant
/// is designed to stay cheap on a store holding 1T+ records: nothing here does
/// a full-keyspace scan or materializes an unbounded result set.
///
/// Operations are prefix-generic over *raw records*: with chunked storage a
/// record is a whole chunk (see `crate::chunk` for the encodings). Build
/// prefixes with the helpers in [`crate::api`] (`task_key_prefix`,
/// `finished_task_key_prefix`, `task_key`, `finished_task_key`). Keys and
/// prefixes starting with `t:` are served from the pending column family,
/// `f:` from the finished one, anything else from the default family. For
/// per-task views use [`get_tasks`] / [`scan_tasks`].
#[derive(Debug, Clone)]
pub enum Query {
    /// Fetch a single record by its exact key. O(1) point read (`db.get`, not a
    /// scan) — the primitive for addressing one known task, e.g. the id returned
    /// by `submit`. `key` must be the full key, built with
    /// [`crate::api::task_key`] / [`crate::api::finished_task_key`]. Unlike
    /// [`Query::Scan`] this matches the key exactly, never by prefix.
    Get { key: String },
    /// Store-wide approximate key count, read from RocksDB SST metadata.
    /// O(1) — never scans. Exact counts over a trillion keys are deliberately
    /// not offered because they cost a full scan.
    EstimateKeys,
    /// Approximate on-disk byte size of the records under `prefix`, from SST
    /// metadata rather than a scan. Useful for capacity planning and deciding
    /// whether a purge is worthwhile. `prefix` must be non-empty.
    EstimateSize { prefix: String },
    /// Stream one bounded page of records under `prefix`, resuming strictly
    /// after `after`. RocksDB seeks directly to the cursor, so the cost is
    /// O(limit) no matter how deep into the keyspace the cursor sits. Drive an
    /// export by looping until [`Page::next`] is `None`.
    Scan {
        prefix: String,
        after: Option<String>,
        limit: usize,
    },
    /// Delete a single record by its exact key. O(1) point delete — the
    /// counterpart to [`Query::Get`] and the single-record analogue of
    /// [`Query::PurgePrefix`]. Idempotent: absent keys are a no-op.
    ///
    /// Operates on the raw store only. To remove a *pending* task that may be
    /// queued or leased, use `cancel`/`complete` instead, so the in-memory dedup
    /// set and lease state stay consistent — this bypasses both.
    Delete { key: String },
    /// Drop every record under `prefix` with a single range tombstone. The
    /// write cost is independent of how many keys match — the alternative,
    /// deleting a billion keys one at a time, is what this exists to avoid.
    /// `prefix` must be non-empty.
    PurgePrefix { prefix: String },
}

/// A single key/value record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
    pub value: Vec<u8>,
}

/// One bounded page of a [`Query::Scan`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    pub records: Vec<Record>,
    /// Cursor for the next page: pass it back as `Query::Scan { after, .. }`.
    /// `None` means the scan is exhausted.
    pub next: Option<String>,
}

/// The outcome of a [`Query`].
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// A single record, or `None` when the key is absent.
    Record(Option<Record>),
    /// Approximate total key count.
    EstimateKeys(u64),
    /// Approximate on-disk size, in bytes.
    EstimateSize(u64),
    /// A page of records.
    Page(Page),
    /// A single-record delete completed.
    Deleted,
    /// A purge completed.
    Purged,
}

pub fn query(api: Arc<ServerAPI>, query: Query) -> Result<QueryResult, Error> {
    match query {
        Query::Get { key } => {
            let value = match api.cf_for_key(key.as_bytes()) {
                Some(cf) => api.db.get_cf(cf, key.as_bytes()),
                None => api.db.get(key.as_bytes()),
            }
            .map_err(|err| Error::io_error(format!("Failed to read record: {err}")))?;
            Ok(QueryResult::Record(
                value.map(|value| Record { key, value }),
            ))
        }
        Query::EstimateKeys => {
            let read = |value: Result<Option<u64>, rust_rocksdb::Error>| {
                value
                    .map(|keys| keys.unwrap_or(0))
                    .map_err(|err| Error::io_error(format!("Failed to read key estimate: {err}")))
            };
            let keys = read(api.db.property_int_value(properties::ESTIMATE_NUM_KEYS))?
                + read(
                    api.db
                        .property_int_value_cf(api.cf_pending(), properties::ESTIMATE_NUM_KEYS),
                )?
                + read(
                    api.db
                        .property_int_value_cf(api.cf_finished(), properties::ESTIMATE_NUM_KEYS),
                )?;
            Ok(QueryResult::EstimateKeys(keys))
        }
        Query::EstimateSize { prefix } => {
            let end = prefix_successor(prefix.as_bytes())
                .ok_or_else(|| Error::invalid_argument("Cannot size an unbounded prefix"))?;
            let range = [Range::new(prefix.as_bytes(), &end)];
            let size = match api.cf_for_key(prefix.as_bytes()) {
                Some(cf) => api.db.get_approximate_sizes_cf(cf, &range),
                None => api.db.get_approximate_sizes(&range),
            }
            .into_iter()
            .next()
            .unwrap_or(0);
            Ok(QueryResult::EstimateSize(size))
        }
        Query::Scan {
            prefix,
            after,
            limit,
        } => {
            // At least one record per page, so the cursor always advances.
            let limit = limit.max(1);
            // Seek straight to the resume point; no scanning from the store start.
            let start = after.as_deref().unwrap_or(prefix.as_str());
            let mode = IteratorMode::From(start.as_bytes(), Direction::Forward);
            let iter = match api.cf_for_key(prefix.as_bytes()) {
                Some(cf) => api.db.iterator_cf(cf, mode),
                None => api.db.iterator(mode),
            };

            let mut records: Vec<Record> = Vec::with_capacity(limit.min(1024));
            let mut next = None;
            for result in iter {
                let record = record_from(result)?;
                if !record.key.starts_with(&prefix) {
                    break;
                }
                // Seeking to the cursor yields it first; resume strictly after it.
                if after.as_deref() == Some(record.key.as_str()) {
                    continue;
                }
                if records.len() >= limit {
                    // One more match exists beyond this page: hand back the last
                    // returned key so the caller resumes right after it.
                    next = records.last().map(|record| record.key.clone());
                    break;
                }
                records.push(record);
            }
            Ok(QueryResult::Page(Page { records, next }))
        }
        Query::Delete { key } => {
            match api.cf_for_key(key.as_bytes()) {
                Some(cf) => api.db.delete_cf(cf, key.as_bytes()),
                None => api.db.delete(key.as_bytes()),
            }
            .map_err(|err| Error::io_error(format!("Failed to delete record: {err}")))?;
            Ok(QueryResult::Deleted)
        }
        Query::PurgePrefix { prefix } => {
            let end = prefix_successor(prefix.as_bytes())
                .ok_or_else(|| Error::invalid_argument("Cannot purge an unbounded prefix"))?;
            let mut batch = WriteBatch::default();
            match api.cf_for_key(prefix.as_bytes()) {
                Some(cf) => batch.delete_range_cf(cf, prefix.as_bytes(), end.as_slice()),
                None => batch.delete_range(prefix.as_bytes(), end.as_slice()),
            }
            api.db
                .write(&batch)
                .map_err(|err| Error::io_error(format!("Failed to purge prefix: {err}")))?;
            Ok(QueryResult::Purged)
        }
    }
}

// ---------------------------------------------------------------------------
// Task-level views over chunk records (used by the wire `Query` surface)
// ---------------------------------------------------------------------------

/// Per-task status of `ids` (task ids of `task_type` on this server):
/// Finished (with the result), Leased or Pending (with the payload), or
/// Unknown (malformed id, another server's id, or no such task). Each chunk is
/// read at most once per call.
pub fn get_tasks(
    api: &ServerAPI,
    task_type: &Identifier,
    ids: Vec<String>,
) -> Result<Vec<TaskRecord>, Error> {
    type Chunk = (Option<FinishedChunk>, Option<ChunkPayloads>);
    let mut chunks: HashMap<u64, Chunk> = HashMap::new();
    let mut records = Vec::with_capacity(ids.len());
    for task_id in ids {
        let Ok(id) = api.parse_task_id(&task_id) else {
            records.push(TaskRecord {
                task_id,
                status: TaskStatus::Unknown,
                bytes: None,
            });
            continue;
        };
        let idx = id.idx as usize;
        let (finished, pending) = match chunks.entry(id.chunk_seq) {
            Entry::Occupied(chunk) => chunk.into_mut(),
            Entry::Vacant(slot) => {
                let finished = api.read_finished(task_type, id.chunk_seq)?;
                let pending = api.read_pending(task_type, id.chunk_seq)?;
                slot.insert((finished, pending))
            }
        };

        let result = finished
            .as_ref()
            .and_then(|finished| finished.results.get(idx).cloned().flatten());
        let (status, bytes) = if let Some(result) = result {
            (TaskStatus::Finished, Some(result))
        } else if let Some(payload) = pending.as_ref().and_then(|pending| pending.get(idx)) {
            let leased = api
                .leased_tasks
                .cell(task_type, id.chunk_seq)
                .is_some_and(|cell| {
                    let lease = lock_lease(&cell);
                    !lease.gone && lease.chunk.open.get(idx)
                });
            let status = if leased {
                TaskStatus::Leased
            } else {
                TaskStatus::Pending
            };
            (status, Some(payload.to_vec()))
        } else {
            (TaskStatus::Unknown, None)
        };
        records.push(TaskRecord {
            task_id,
            status,
            bytes,
        });
    }
    Ok(records)
}

/// One page of *tasks* (not chunks) of one scope, in task id order, resuming
/// strictly after the task id `after`. `Pending` lists tasks without a result
/// (with their payload); `Finished` lists tasks with a result. `next` is the
/// last returned task id when more remain.
pub fn scan_tasks(
    api: &ServerAPI,
    task_type: &Identifier,
    scope: Scope,
    after: Option<&str>,
    limit: usize,
) -> Result<(Vec<PageRecord>, Option<String>), Error> {
    let limit = limit.max(1);
    let after = after.map(|after| api.parse_task_id(after)).transpose()?;
    let (cf, prefix, start) = match scope {
        Scope::Pending => (
            api.cf_pending(),
            task_key_prefix(task_type),
            after.map(|id| task_key(task_type, id.chunk_seq)),
        ),
        Scope::Finished => (
            api.cf_finished(),
            finished_task_key_prefix(task_type),
            after.map(|id| finished_task_key(task_type, id.chunk_seq)),
        ),
    };
    let mut opts = ReadOptions::default();
    opts.set_iterate_lower_bound(start.unwrap_or_else(|| prefix.clone()).into_bytes());
    if let Some(upper) = prefix_successor(prefix.as_bytes()) {
        opts.set_iterate_upper_bound(upper);
    }

    let mut records: Vec<PageRecord> = Vec::with_capacity(limit.min(1024));
    for result in api.db.iterator_cf_opt(cf, opts, IteratorMode::Start) {
        let (key, value) =
            result.map_err(|err| Error::io_error(format!("Failed to read record: {err}")))?;
        let Some(chunk_seq) = chunk_seq_from_key(&key) else {
            continue;
        };
        // Tasks of this chunk to list, with their bytes.
        let tasks: Vec<(usize, Vec<u8>)> = match scope {
            Scope::Pending => {
                let payloads = ChunkPayloads::decode(value.into_vec())?;
                let done = match api.read_finished(task_type, chunk_seq)? {
                    Some(finished) => finished.done(),
                    None => Bitmap::empty(payloads.len()),
                };
                (0..payloads.len())
                    .filter(|idx| !done.get(*idx))
                    .filter_map(|idx| payloads.get(idx).map(|p| (idx, p.to_vec())))
                    .collect()
            }
            Scope::Finished => FinishedChunk::decode(&value)?
                .results
                .into_iter()
                .enumerate()
                .filter_map(|(idx, result)| result.map(|result| (idx, result)))
                .collect(),
        };
        for (idx, bytes) in tasks {
            if after.is_some_and(|after| (chunk_seq, idx as u16) <= (after.chunk_seq, after.idx)) {
                continue;
            }
            if records.len() >= limit {
                // One more task exists beyond this page.
                let next = records.last().map(|record| record.task_id.clone());
                return Ok((records, next));
            }
            records.push(PageRecord {
                task_id: api.task_id(chunk_seq, idx as u16),
                bytes,
            });
        }
    }
    Ok((records, None))
}

/// Deletes the chunks holding `ids` from one scope (chunk granularity: every
/// task of a named chunk goes). Refused with InvalidArgument `"TaskInFlight"`
/// if any named chunk is queued or leased — those must go through
/// cancel/complete so the dedup and lease state stay consistent.
pub fn delete_tasks(
    api: &ServerAPI,
    task_type: &Identifier,
    scope: Scope,
    ids: &[String],
) -> Result<(), Error> {
    let mut chunks = Vec::with_capacity(ids.len());
    for task_id in ids {
        chunks.push((task_id, api.parse_task_id(task_id)?.chunk_seq));
    }
    if let Some(queue) = api.task_queue.get(task_type)
        && let Some((task_id, _)) = chunks
            .iter()
            .find(|(_, chunk_seq)| queue.dedup.contains(chunk_seq))
    {
        return Err(Error::invalid_argument(format!("TaskInFlight: {task_id}")));
    }
    let mut batch = WriteBatch::default();
    for (_, chunk_seq) in chunks {
        match scope {
            Scope::Pending => batch.delete_cf(api.cf_pending(), task_key(task_type, chunk_seq)),
            Scope::Finished => {
                batch.delete_cf(api.cf_finished(), finished_task_key(task_type, chunk_seq))
            }
        }
    }
    api.db
        .write(&batch)
        .map_err(|err| Error::io_error(format!("Failed to delete records: {err}")))
}

/// The raw key prefix of one scope of a task type.
pub fn scope_prefix(task_type: &Identifier, scope: Scope) -> String {
    match scope {
        Scope::Pending => task_key_prefix(task_type),
        Scope::Finished => finished_task_key_prefix(task_type),
    }
}

/// Smallest key strictly greater than every key with `prefix`, i.e. the
/// exclusive upper bound of the prefix range. `None` when `prefix` is empty or
/// all `0xff` (an unbounded range with no finite successor).
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xff {
            *last += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

type DbEntry = Result<(Box<[u8]>, Box<[u8]>), rust_rocksdb::Error>;

fn record_from(result: DbEntry) -> Result<Record, Error> {
    let (key, value) =
        result.map_err(|err| Error::io_error(format!("Failed to read record: {err}")))?;
    let key = String::from_utf8(key.into_vec())
        .map_err(|err| Error::io_error(format!("Non-UTF8 key in store: {err}")))?;
    Ok(Record {
        key,
        value: value.into_vec(),
    })
}
