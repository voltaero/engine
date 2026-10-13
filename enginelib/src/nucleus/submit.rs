use std::sync::Arc;

use rust_rocksdb::WriteBatch;

use crate::api::task_key;
use crate::chunk::encode_pending;
use crate::{Identifier, Registry, api::ServerAPI, error::Error};

// t:namespace:task_name:<chunk_seq> -> pending chunk record (payloads)
// f:namespace:task_name:<chunk_seq> -> finished chunk record (results + done bitmap)

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
    let fail_all = |results: &mut Vec<Result<String, Error>>, err: Error| {
        for result in results.iter_mut().filter(|result| result.is_ok()) {
            *result = Err(err.clone());
        }
    };
    // Held until the write lands: the refill cursor never passes a seq whose
    // record may still be written.
    let reservation = match api.chunk_seq.reserve(&api.db, chunks.len() as u64) {
        Ok(reservation) => reservation,
        Err(err) => {
            fail_all(&mut results, err);
            return Ok(results);
        }
    };

    let cf = api.cf_pending();
    let mut batch = WriteBatch::default();
    for (offset, members) in chunks.iter().enumerate() {
        let chunk_seq = reservation.start + offset as u64;
        let record = encode_pending(members.iter().map(|&i| payloads[i].as_ref()));
        batch.put_cf(cf, task_key(&task_type, chunk_seq), record);
        for (idx, &i) in members.iter().enumerate() {
            results[i] = Ok(api.task_id(chunk_seq, idx as u16));
        }
    }
    let written = api.db.write(&batch);
    drop(reservation);
    if let Err(err) = written {
        fail_all(&mut results, Error::io_error(err.to_string()));
        return Ok(results);
    }

    // Wake the refill path: new chunks are loadable. Only kick a refill when
    // the queue is running low; otherwise lease() will as it drains.
    api.load_state.get(&task_type).note_submitted();
    ServerAPI::refill_if_low(&api, &task_type);
    Ok(results)
}
