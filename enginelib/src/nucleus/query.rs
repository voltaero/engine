use std::sync::Arc;

use rust_rocksdb::{Direction, IteratorMode, Range, WriteBatch, properties};
use serde::{Deserialize, Serialize};

use crate::{api::ServerAPI, error::Error};

/// Administrative / export operations over the task store.
///
/// This is the operator-facing surface — inspect, back up, and prune records —
/// distinct from the task lifecycle (submit / lease / complete). Every variant
/// is designed to stay cheap on a store holding 1T+ records: nothing here does
/// a full-keyspace scan or materializes an unbounded result set.
///
/// Operations are prefix-generic. Build prefixes with the helpers in
/// [`crate::api`] (`task_key_prefix`, `finished_task_key_prefix`, `task_key`,
/// `finished_task_key`) to scope to a task type, its pending records, its
/// finished records, or a single record.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            let value = api
                .db
                .get(key.as_bytes())
                .map_err(|err| Error::io_error(format!("Failed to read record: {err}")))?;
            Ok(QueryResult::Record(
                value.map(|value| Record { key, value }),
            ))
        }
        Query::EstimateKeys => {
            let keys = api
                .db
                .property_int_value(properties::ESTIMATE_NUM_KEYS)
                .map_err(|err| Error::io_error(format!("Failed to read key estimate: {err}")))?
                .unwrap_or(0);
            Ok(QueryResult::EstimateKeys(keys))
        }
        Query::EstimateSize { prefix } => {
            let end = prefix_successor(prefix.as_bytes())
                .ok_or_else(|| Error::invalid_argument("Cannot size an unbounded prefix"))?;
            let size = api
                .db
                .get_approximate_sizes(&[Range::new(prefix.as_bytes(), &end)])
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
            let iter = api
                .db
                .iterator(IteratorMode::From(start.as_bytes(), Direction::Forward));

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
            api.db
                .delete(key.as_bytes())
                .map_err(|err| Error::io_error(format!("Failed to delete record: {err}")))?;
            Ok(QueryResult::Deleted)
        }
        Query::PurgePrefix { prefix } => {
            let end = prefix_successor(prefix.as_bytes())
                .ok_or_else(|| Error::invalid_argument("Cannot purge an unbounded prefix"))?;
            let mut batch = WriteBatch::default();
            batch.delete_range(prefix.as_bytes(), end.as_slice());
            api.db
                .write(&batch)
                .map_err(|err| Error::io_error(format!("Failed to purge prefix: {err}")))?;
            Ok(QueryResult::Purged)
        }
    }
}

/// Smallest key strictly greater than every key with `prefix`, i.e. the
/// exclusive upper bound of the prefix range. `None` when `prefix` is empty or
/// all `0xff` (an unbounded range with no finite successor).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
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
