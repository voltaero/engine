use std::collections::HashMap;
use std::sync::Arc;

use rust_rocksdb::WriteBatch;

use crate::api::{TaskSettings, finished_task_key, lock_lease, task_key};
use crate::chunk::{Bitmap, decode_finished, encode_finished};
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

impl CompleteItem {
    pub fn as_ref(&self) -> CompleteRef<'_> {
        CompleteRef {
            task_type: (&self.task_type.0, &self.task_type.1),
            task_id: &self.task_id,
            lease_token: self.lease_token,
            bytes: &self.bytes,
        }
    }
}

/// Borrowed form of [`CompleteItem`], e.g. decoded straight out of a request
/// buffer: completing never copies a result before it is encoded into the
/// chunk's finished record.
#[derive(Debug, Clone, Copy)]
pub struct CompleteRef<'a> {
    pub task_type: (&'a str, &'a str),
    pub task_id: &'a str,
    pub lease_token: u64,
    pub bytes: &'a [u8],
}

/// Finalizes one leased task with its result bytes. See [`complete_batch`].
pub fn complete(
    api: Arc<ServerAPI>,
    task_type: Identifier,
    task_id: String,
    lease_token: u64,
    task_bytes: &[u8],
) -> Result<(), Error> {
    complete_refs(
        &api,
        [CompleteRef {
            task_type: (&task_type.0, &task_type.1),
            task_id: &task_id,
            lease_token,
            bytes: task_bytes,
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
    complete_refs(api, items.iter().map(CompleteItem::as_ref))
}

/// [`complete_batch`] over borrowed items.
pub fn complete_refs<'a>(
    api: &Arc<ServerAPI>,
    items: impl IntoIterator<Item = CompleteRef<'a>>,
) -> Vec<Result<(), Error>> {
    let items = items.into_iter();
    let mut results: Vec<Result<(), Error>> = Vec::with_capacity(items.size_hint().0);
    // Task types seen in this call, looked up by the borrowed names (no owned
    // key per item). A batch rarely has more than one, so the previous item's
    // type is checked first and the map is only hashed when the type changes.
    let mut types: Vec<(Identifier, Option<TypeInfo>)> = Vec::new();
    let mut type_index: HashMap<(&'a str, &'a str), usize> = HashMap::new();
    let mut last_type: Option<((&'a str, &'a str), usize)> = None;
    // Groups keyed by (type, chunk, token), in first-seen order. Items of one
    // chunk usually arrive together, so the last group is checked first.
    let mut index: HashMap<GroupKey, usize> = HashMap::new();
    let mut groups: Vec<(GroupKey, Vec<Entry<'a>>)> = Vec::new();

    for (pos, item) in items.enumerate() {
        let checked = (|| {
            let ty = match last_type {
                Some((name, ty)) if name == item.task_type => ty,
                _ => {
                    let ty = *type_index.entry(item.task_type).or_insert_with(|| {
                        let (a, b) = item.task_type;
                        let task_type = (a.to_owned(), b.to_owned());
                        let info = api
                            .task_registry
                            .get(&task_type)
                            .map(|task| (task, api.settings(&task_type)));
                        types.push((task_type, info));
                        types.len() - 1
                    });
                    last_type = Some((item.task_type, ty));
                    ty
                }
            };
            let (task, settings) = types[ty]
                .1
                .as_ref()
                .ok_or(Error::not_found("TaskTypeNotFound"))?;
            let id = api.parse_task_id(item.task_id)?;
            if settings
                .max_result
                .is_some_and(|max| item.bytes.len() > max as usize)
            {
                return Err(Error::invalid_argument("ResultTooLarge"));
            }
            if !task.verify(item.bytes) {
                return Err(Error::invalid_argument("Failed to verify task bytes"));
            }
            Ok((ty, id))
        })();
        match checked {
            Ok((ty, id)) => {
                results.push(Ok(()));
                let key = (ty, id.chunk_seq, item.lease_token);
                let group = match groups.last() {
                    Some((last, _)) if *last == key => groups.len() - 1,
                    _ => *index.entry(key).or_insert_with(|| {
                        groups.push((key, Vec::new()));
                        groups.len() - 1
                    }),
                };
                groups[group].1.push(Entry {
                    pos,
                    idx: id.idx as usize,
                    bytes: item.bytes,
                });
            }
            Err(err) => results.push(Err(err)),
        }
    }

    for ((ty, chunk_seq, token), entries) in groups {
        complete_chunk(api, &types[ty].0, chunk_seq, token, entries, &mut results);
    }
    results
}

/// (index into the call's task types, chunk_seq, lease token)
type GroupKey = (usize, u64, u64);
type TypeInfo = (Box<dyn Task>, TaskSettings);

struct Entry<'a> {
    pos: usize,
    idx: usize,
    bytes: &'a [u8],
}

/// Reads the raw finished record of a chunk (pinned, not copied).
fn read_finished_raw<'db>(
    api: &'db ServerAPI,
    task_type: &Identifier,
    chunk_seq: u64,
) -> Result<Option<rust_rocksdb::DBPinnableSlice<'db>>, Error> {
    api.db
        .get_pinned_cf(api.cf_finished(), finished_task_key(task_type, chunk_seq))
        .map_err(|err| Error::io_error(format!("Failed to read finished chunk: {err}")))
}

fn complete_chunk(
    api: &Arc<ServerAPI>,
    task_type: &Identifier,
    chunk_seq: u64,
    token: u64,
    entries: Vec<Entry<'_>>,
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
    // They stay in the pinned record; `slots` borrows them.
    let needs_stored = lease.chunk.has_finished || retries.iter().any(|e| !newly.get(e.idx));
    let stored = if needs_stored {
        match read_finished_raw(api, task_type, chunk_seq) {
            Ok(stored) => stored,
            Err(err) => {
                for entry in fresh.iter().chain(&retries) {
                    results[entry.pos] = Err(err.clone());
                }
                return;
            }
        }
    } else {
        None
    };
    let mut slots = match stored.as_deref().map(decode_finished).transpose() {
        Ok(slots) => slots.unwrap_or_default(),
        Err(err) => {
            for entry in fresh.iter().chain(&retries) {
                results[entry.pos] = Err(err.clone());
            }
            return;
        }
    };
    slots.resize(len, None);

    // Retries of tasks done before this call: compare with what is stored.
    let mut in_call: Vec<Entry> = Vec::new();
    for entry in retries {
        if newly.get(entry.idx) {
            in_call.push(entry);
        } else {
            results[entry.pos] = match slots[entry.idx] {
                Some(stored) if stored == entry.bytes => Ok(()),
                _ => Err(Error::not_found("TaskNotLeased")),
            };
        }
    }

    if fresh.is_empty() {
        return;
    }

    let mut open_after = lease.chunk.open.clone();
    for entry in &fresh {
        open_after.clear(entry.idx);
        slots[entry.idx] = Some(entry.bytes);
    }
    let all_done = open_after.none();

    // Results and (when the chunk is done) the pending delete land atomically,
    // so a crash can neither lose results nor leave a done chunk pending.
    let mut batch = WriteBatch::default();
    batch.put_cf(
        api.cf_finished(),
        finished_task_key(task_type, chunk_seq),
        encode_finished(slots.iter().copied()),
    );
    if all_done {
        batch.delete_cf(api.cf_pending(), task_key(task_type, chunk_seq));
    }
    if let Err(err) = api.db.write(&batch) {
        // Nothing changed: the lease is intact and the worker may retry.
        let err = Error::io_error(format!("Failed to persist results: {err}"));
        for entry in fresh.iter().chain(&in_call) {
            results[entry.pos] = Err(err.clone());
        }
        return;
    }
    for entry in in_call {
        if slots[entry.idx] != Some(entry.bytes) {
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
    entries: Vec<Entry<'_>>,
    results: &mut [Result<(), Error>],
    otherwise: Option<Error>,
) {
    if entries.is_empty() {
        return;
    }
    let otherwise = otherwise.unwrap_or_else(|| Error::not_found("TaskNotLeased"));
    let stored = read_finished_raw(api, task_type, chunk_seq);
    let slots = match &stored {
        Ok(stored) => stored.as_deref().map(decode_finished).transpose(),
        Err(err) => Err(err.clone()),
    };
    let slots = match &slots {
        Ok(slots) => slots.as_deref().unwrap_or_default(),
        Err(err) => {
            for entry in entries {
                results[entry.pos] = Err(err.clone());
            }
            return;
        }
    };
    for entry in entries {
        results[entry.pos] = match slots.get(entry.idx) {
            Some(Some(stored)) if *stored == entry.bytes => Ok(()),
            _ => Err(otherwise.clone()),
        };
    }
}
