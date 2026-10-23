use std::sync::Arc;

use rust_rocksdb::WriteBatch;

use crate::api::task_key;
use crate::chunk::{DerivedRange, encode_derived, encode_pending};
use crate::{Identifier, Registry, api::ServerAPI, error::Error};

// t:namespace:task_name:<chunk_seq> -> pending chunk record (payloads)
// f:namespace:task_name:<chunk_seq> -> finished chunk record (results + done bitmap)
// A derived type's t: record is (base, index range) and its f: the chunk result.

/// Submits one task. Equivalent to a [`submit_batch`] of one payload, so it
/// gets a chunk of its own; batch submits to amortize storage.
pub fn submit(
    api: Arc<ServerAPI>,
    task_bytes: &[u8],
    task_type: Identifier,
) -> Result<String, Error> {
    submit_batch(api, &[task_bytes], task_type)?
        .pop()
        .unwrap_or_else(|| Err(Error::new("submit_batch returned no result".into())))
}

/// Verifies every payload, packs the valid ones into fresh chunks of the
/// type's `chunk_size` (a partial last chunk is fine) and persists all chunks
/// in a single `WriteBatch`. Returns one result per payload, in order: the
/// task id, or why the payload was rejected. Ids are only returned once the
/// batch is durable; if the write fails every valid payload gets that error.
///
/// Chunks are never shared across calls, which keeps that guarantee without a
/// flush timer; callers should batch their inserts.
pub fn submit_batch<P: AsRef<[u8]>>(
    api: Arc<ServerAPI>,
    payloads: &[P],
    task_type: Identifier,
) -> Result<Vec<Result<String, Error>>, Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;
    let settings = api.settings(&task_type);
    if settings.derived {
        return Err(Error::invalid_argument("DerivedTaskType"));
    }

    let mut results: Vec<Result<String, Error>> = Vec::with_capacity(payloads.len());
    let mut valid: Vec<usize> = Vec::with_capacity(payloads.len());
    for (i, payload) in payloads.iter().enumerate() {
        let payload = payload.as_ref();
        if settings
            .max_payload
            .is_some_and(|max| payload.len() > max as usize)
        {
            results.push(Err(Error::invalid_argument("PayloadTooLarge")));
        } else if !task.verify(payload) {
            results.push(Err(Error::invalid_argument("Failed to verify task bytes")));
        } else {
            valid.push(i);
            results.push(Ok(String::new()));
        }
    }
    if valid.is_empty() {
        return Ok(results);
    }

    let chunks: Vec<&[usize]> = valid.chunks(settings.chunk_size as usize).collect();
    let records = chunks
        .iter()
        .map(|members| encode_pending(members.iter().map(|&i| payloads[i].as_ref())));
    match write_chunks(&api, &task_type, chunks.len(), records) {
        Ok(start) => {
            for (offset, members) in chunks.iter().enumerate() {
                for (idx, &i) in members.iter().enumerate() {
                    results[i] = Ok(api.task_id(start + offset as u64, idx as u16));
                }
            }
        }
        Err(err) => {
            for result in results.iter_mut().filter(|result| result.is_ok()) {
                *result = Err(err.clone());
            }
        }
    }
    Ok(results)
}

/// Most chunks one [`submit_derived`] call may create.
pub const MAX_DERIVED_CHUNKS: u64 = 65_536;
/// Most bytes of chunk records one [`submit_derived`] call may write. Every
/// chunk stores its own copy of `base`, so this bounds the amplification of one
/// small request into a huge in-memory `WriteBatch`.
pub const MAX_DERIVED_BATCH_BYTES: u64 = 64 * 1024 * 1024;
/// Fixed bytes of a derived chunk record besides its base.
const DERIVED_RECORD_OVERHEAD: u64 = 17;

/// Submits the task indexes `start..start + count` of a derived type (see
/// [`crate::task::TaskLimits::derived`]): `base` is verified once, then the
/// range is cut into chunks of `chunk_size` indexes, each stored as just
/// `(base, sub-range)` and written in one `WriteBatch`. Returns one id per
/// chunk, in range order; a chunk is completed with one result under that id.
pub fn submit_derived(
    api: Arc<ServerAPI>,
    base: &[u8],
    start: u64,
    count: u64,
    task_type: Identifier,
) -> Result<Vec<String>, Error> {
    let task = api
        .task_registry
        .get(&task_type)
        .ok_or(Error::not_found("TaskTypeNotFound"))?;
    let settings = api.settings(&task_type);
    if !settings.derived {
        return Err(Error::invalid_argument("NotDerivedTaskType"));
    }
    if count == 0 || start.checked_add(count).is_none() {
        return Err(Error::invalid_argument("InvalidRange"));
    }
    let chunk_size = u64::from(settings.chunk_size);
    let n = count.div_ceil(chunk_size);
    if n > MAX_DERIVED_CHUNKS {
        return Err(Error::invalid_argument("TooManyChunks"));
    }
    let record_bytes = (base.len() as u64).saturating_add(DERIVED_RECORD_OVERHEAD);
    if n.saturating_mul(record_bytes) > MAX_DERIVED_BATCH_BYTES {
        return Err(Error::invalid_argument("InsertTooLarge"));
    }
    if settings
        .max_payload
        .is_some_and(|max| base.len() > max as usize)
    {
        return Err(Error::invalid_argument("PayloadTooLarge"));
    }
    if !task.verify(base) {
        return Err(Error::invalid_argument("Failed to verify task bytes"));
    }

    let records = (0..n).map(|k| {
        let offset = k * chunk_size;
        let range = DerivedRange {
            start: start + offset,
            count: chunk_size.min(count - offset),
        };
        encode_derived(base, range)
    });
    let first = write_chunks(&api, &task_type, n as usize, records)?;
    Ok((0..n).map(|k| api.task_id(first + k, 0)).collect())
}

/// Reserves `n` fresh seqs, writes `records` (exactly `n` of them) as the
/// pending chunks of `task_type` in one `WriteBatch` and wakes the refill.
/// Returns the first seq; nothing is visible to the caller until the write is
/// durable.
fn write_chunks(
    api: &Arc<ServerAPI>,
    task_type: &Identifier,
    n: usize,
    records: impl Iterator<Item = Vec<u8>>,
) -> Result<u64, Error> {
    // Held until the write lands: the refill cursor never passes a seq whose
    // record may still be written.
    let reservation = api.chunk_seq.reserve(&api.db, n as u64)?;
    let start = reservation.start;

    let cf = api.cf_pending();
    let mut batch = WriteBatch::default();
    for (offset, record) in records.enumerate() {
        batch.put_cf(cf, task_key(task_type, start + offset as u64), record);
    }
    let written = api.db.write(&batch);
    drop(reservation);
    // Releasing the reservation may advance `safe_upto` past chunks that other
    // inserts (of any type) already wrote: refill types whose pass stopped here.
    ServerAPI::wake_capped(api);
    written.map_err(|err| Error::io_error(err.to_string()))?;

    // Wake the refill path: new chunks are loadable. Only kick a refill when
    // the queue is running low; otherwise lease() will as it drains.
    api.load_state.get(task_type).note_submitted();
    ServerAPI::refill_if_low(api, task_type);
    Ok(start)
}
