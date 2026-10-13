use std::collections::HashMap;
use std::sync::Arc;

use rust_rocksdb::WriteBatch;

use crate::api::{TaskSettings, finished_task_key, lock_lease, task_key};
use crate::chunk::{Bitmap, FinishedChunk};
use crate::task::Task;
use crate::{Identifier, Registry, api::ServerAPI, error::Error};

/// One result handed to [`complete_batch`].
#[derive(Debug, Clone)]
pub struct CompleteItem {
    pub task_type: Identifier,
    pub task_id: String,
    pub lease_token: u64,
    pub bytes: Vec<u8>,
}

/// Finalizes one leased task with its result bytes. See [`complete_batch`].
pub fn complete(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: String,
    lease_token: u64,
    task_bytes: &[u8],
) -> Result<(), Error> {
    complete_batch(
        &api,
        vec![CompleteItem {
            task_type,
            task_id,
            lease_token,
            bytes: task_bytes.to_vec(),
        }],
    )
    .pop()
    .unwrap_or_else(|| Err(Error::new("complete_batch returned no result".into())))
}

/// Finalizes leased tasks with their results; one result per item, in order.
///
/// Every result goes through the type's `verify` (and `max_result`). Items are
/// grouped per chunk lease; each group is merged into the chunk's finished
/// (`f:`) record in one atomic `WriteBatch`. A group may cover any subset of the
/// lease's tasks (partial complete). When the last open task of a chunk is
/// done, the same batch deletes the pending (`t:`) record and the lease and
/// dedup entry are dropped. Only the current lease holder (matching token) may
/// complete.
///
/// Idempotent for retries: a task whose identical result is already stored
/// (an earlier call succeeded and its response was lost) succeeds without a
/// write, with or without a live lease. A different result for a done task is
/// NotFound `"TaskNotLeased"` (or `"LeaseTokenMismatch"` if someone else holds
/// the chunk's lease).
pub fn complete_batch(api: &Arc<ServerAPI>, items: Vec<CompleteItem>) -> Vec<Result<(), Error>> {
    let mut results: Vec<Result<(), Error>> = Vec::with_capacity(items.len());
    // Groups keyed by (type, chunk, token), in first-seen order.
    let mut index: HashMap<GroupKey, usize> = HashMap::new();
    let mut groups: Vec<(GroupKey, Vec<Entry>)> = Vec::new();
    let mut types: HashMap<Identifier, Option<TypeInfo>> = HashMap::new();

    for (pos, item) in items.into_iter().enumerate() {
        let checked = (|| {
            let typed = types
                .entry(item.task_type.clone())
                .or_insert_with_key(|task_type| {
                    api.task_registry
                        .get(task_type)
                        .map(|task| (task, api.settings(task_type)))
                });
            let (task, settings) = typed.as_ref().ok_or(Error::not_found("TaskTypeNotFound"))?;
            let id = api.parse_task_id(&item.task_id)?;
            if settings
                .max_result
                .is_some_and(|max| item.bytes.len() > max as usize)
            {
                return Err(Error::invalid_argument("ResultTooLarge"));
            }
            if !task.verify(&item.bytes) {
                return Err(Error::invalid_argument("Failed to verify task bytes"));
            }
            Ok(id)
        })();
        match checked {
            Ok(id) => {
                results.push(Ok(()));
                let key = (item.task_type, id.chunk_seq, item.lease_token);
                let group = *index.entry(key.clone()).or_insert_with(|| {
                    groups.push((key, Vec::new()));
                    groups.len() - 1
                });
                groups[group].1.push(Entry {
                    pos,
                    idx: id.idx as usize,
                    bytes: item.bytes,
                });
            }
            Err(err) => results.push(Err(err)),
        }
    }

    for ((task_type, chunk_seq, token), entries) in groups {
        complete_chunk(api, &task_type, chunk_seq, token, entries, &mut results);
    }
    results
}

/// (task type, chunk_seq, lease token)
type GroupKey = (Identifier, u64, u64);
type TypeInfo = (Box<dyn Task>, TaskSettings);

struct Entry {
    pos: usize,
    idx: usize,
    bytes: Vec<u8>,
}

fn complete_chunk(
    api: &Arc<ServerAPI>,
    task_type: &Identifier,
    chunk_seq: u64,
    token: u64,
    entries: Vec<Entry>,
    results: &mut [Result<(), Error>],
) {
    let Some(cell) = api.leased_tasks.cell(task_type, chunk_seq) else {
        return already_completed(api, task_type, chunk_seq, entries, results, None);
    };
    let mut lease = lock_lease(&cell);
    if lease.gone {
        drop(lease);
        return already_completed(api, task_type, chunk_seq, entries, results, None);
    }
    if lease.token != token {
        // Someone else holds the chunk. Tasks still open in that lease are
        // theirs; done ones may be a stale holder's idempotent retry.
        let (open, done): (Vec<Entry>, Vec<Entry>) = entries
            .into_iter()
            .partition(|entry| lease.chunk.open.get(entry.idx));
        let mismatch = Error::invalid_argument("LeaseTokenMismatch");
        for entry in open {
            results[entry.pos] = Err(mismatch.clone());
        }
        drop(lease);
        return already_completed(api, task_type, chunk_seq, done, results, Some(mismatch));
    }

    let len = lease.chunk.payloads.len();
    // Split into results for open tasks and retries of done ones. A task named
    // twice in one call counts once; the duplicate must carry the same bytes.
    let mut fresh: Vec<Entry> = Vec::with_capacity(entries.len());
    let mut retries: Vec<Entry> = Vec::new();
    let mut newly = Bitmap::empty(len);
    for entry in entries {
        if entry.idx >= len {
            results[entry.pos] = Err(Error::not_found("TaskNotFound"));
        } else if lease.chunk.open.get(entry.idx) && !newly.get(entry.idx) {
            newly.set(entry.idx);
            fresh.push(entry);
        } else {
            retries.push(entry);
        }
    }

    // The stored results are only needed to merge into, or to check retries.
    let needs_stored = lease.chunk.has_finished || retries.iter().any(|e| !newly.get(e.idx));
    let mut finished = if needs_stored {
        match api.read_finished(task_type, chunk_seq) {
            Ok(finished) => finished.unwrap_or_else(|| FinishedChunk::new(len)),
            Err(err) => {
                for entry in fresh.iter().chain(&retries) {
                    results[entry.pos] = Err(err.clone());
                }
                return;
            }
        }
    } else {
        FinishedChunk::new(len)
    };
    if finished.results.len() != len {
        finished.results.resize(len, None);
    }

    // Retries of tasks done before this call: compare with what is stored.
    let mut in_call: Vec<Entry> = Vec::new();
    for entry in retries {
        if newly.get(entry.idx) {
            in_call.push(entry);
        } else {
            results[entry.pos] = match &finished.results[entry.idx] {
                Some(stored) if *stored == entry.bytes => Ok(()),
                _ => Err(Error::not_found("TaskNotLeased")),
            };
        }
    }

    if fresh.is_empty() {
        return;
    }

    let mut open_after = lease.chunk.open.clone();
    let mut positions = Vec::with_capacity(fresh.len());
    for entry in fresh {
        open_after.clear(entry.idx);
        positions.push(entry.pos);
        finished.results[entry.idx] = Some(entry.bytes);
    }
    let all_done = open_after.none();

    // Results and (when the chunk is done) the pending delete land atomically,
    // so a crash can neither lose results nor leave a done chunk pending.
    let mut batch = WriteBatch::default();
    batch.put_cf(
        api.cf_finished(),
        finished_task_key(task_type, chunk_seq),
        finished.encode(),
    );
    if all_done {
        batch.delete_cf(api.cf_pending(), task_key(task_type, chunk_seq));
    }
    if let Err(err) = api.db.write(&batch) {
        // Nothing changed: the lease is intact and the worker may retry.
        let err = Error::io_error(format!("Failed to persist results: {err}"));
        for pos in positions.into_iter().chain(in_call.iter().map(|e| e.pos)) {
            results[pos] = Err(err.clone());
        }
        return;
    }
    for entry in in_call {
        if finished.results[entry.idx].as_deref() != Some(entry.bytes.as_slice()) {
            results[entry.pos] = Err(Error::not_found("TaskNotLeased"));
        }
    }

    lease.chunk.open = open_after;
    lease.chunk.has_finished = true;
    if all_done {
        // The pending record is gone: drop the lease and the dedup entry.
        api.leased_tasks.end(task_type, &cell, &mut lease);
        drop(lease);
        if let Some(queue) = api.task_queue.get(task_type) {
            queue.dedup.remove(&chunk_seq);
        }
    }
}

/// No live lease matched: each entry succeeds only if it retries a completion
/// that already stored the identical result; otherwise it fails with
/// `otherwise` (default NotFound `"TaskNotLeased"`).
fn already_completed(
    api: &ServerAPI,
    task_type: &Identifier,
    chunk_seq: u64,
    entries: Vec<Entry>,
    results: &mut [Result<(), Error>],
    otherwise: Option<Error>,
) {
    if entries.is_empty() {
        return;
    }
    let otherwise = otherwise.unwrap_or_else(|| Error::not_found("TaskNotLeased"));
    let finished = match api.read_finished(task_type, chunk_seq) {
        Ok(finished) => finished,
        Err(err) => {
            for entry in entries {
                results[entry.pos] = Err(err.clone());
            }
            return;
        }
    };
    for entry in entries {
        let stored = finished
            .as_ref()
            .and_then(|finished| finished.results.get(entry.idx))
            .and_then(Option::as_deref);
        results[entry.pos] = match stored {
            Some(stored) if stored == entry.bytes.as_slice() => Ok(()),
            _ => Err(otherwise.clone()),
        };
    }
}
