//! Chunked task storage primitives: task ids, chunk record encodings, the
//! open/done bitmap and the persisted chunk sequence allocator.
//!
//! Tasks are stored and leased in *chunks*. Every task is still an individual
//! payload with its own verified result; only the storage and lease unit is a
//! chunk. See `docs/protocol.md` ("Chunked storage") for the layout.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Mutex;

use crate::error::Error;

/// Largest number of tasks in one chunk (`idx` is a `u16`).
pub const MAX_CHUNK_SIZE: u32 = 1 << 16;

/// Length of a task id on the wire: 12 bytes, hex encoded.
pub const TASK_ID_HEX_LEN: usize = 24;

/// A task id: `[server_id u16][chunk_seq u64][idx u16]`, 12 bytes big-endian,
/// hex-encoded as the wire `task_id` (24 lowercase hex chars).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId {
    pub server_id: u16,
    pub chunk_seq: u64,
    pub idx: u16,
}

impl TaskId {
    pub fn new(server_id: u16, chunk_seq: u64, idx: u16) -> Self {
        Self {
            server_id,
            chunk_seq,
            idx,
        }
    }

    /// Parses a 24-char hex task id. Anything else is InvalidArgument
    /// `"InvalidTaskId"`.
    pub fn parse(s: &str) -> Result<Self, Error> {
        let invalid = || Error::invalid_argument(format!("InvalidTaskId: {s}"));
        if s.len() != TASK_ID_HEX_LEN || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        let server_id = u16::from_str_radix(&s[0..4], 16).map_err(|_| invalid())?;
        let chunk_seq = u64::from_str_radix(&s[4..20], 16).map_err(|_| invalid())?;
        let idx = u16::from_str_radix(&s[20..24], 16).map_err(|_| invalid())?;
        Ok(Self::new(server_id, chunk_seq, idx))
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04x}{:016x}{:04x}",
            self.server_id, self.chunk_seq, self.idx
        )
    }
}

/// Zero-padded hex of a chunk sequence number, as used in record keys. Fixed
/// width, so lexicographic key order equals numeric `chunk_seq` order.
pub fn seq_hex(chunk_seq: u64) -> String {
    format!("{chunk_seq:016x}")
}

/// Parses the `chunk_seq` suffix of a record key.
pub fn seq_from_hex(s: &str) -> Option<u64> {
    if s.len() != 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

// ---------------------------------------------------------------------------
// Bitmap
// ---------------------------------------------------------------------------

/// Fixed-length bitset over the task indexes of one chunk.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Bitmap {
    len: usize,
    words: Vec<u64>,
}

impl Bitmap {
    pub fn empty(len: usize) -> Self {
        Self {
            len,
            words: vec![0; len.div_ceil(64)],
        }
    }

    pub fn full(len: usize) -> Self {
        let mut bitmap = Self::empty(len);
        for (i, word) in bitmap.words.iter_mut().enumerate() {
            let bits = (len - i * 64).min(64);
            *word = if bits == 64 {
                u64::MAX
            } else {
                (1 << bits) - 1
            };
        }
        bitmap
    }

    /// Number of indexes the bitmap covers (set or not).
    pub fn len(&self) -> usize {
        self.len
    }

    /// True when the bitmap covers no indexes at all (see [`Bitmap::none`]
    /// for "no bit set").
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, idx: usize) -> bool {
        idx < self.len && self.words[idx / 64] & (1 << (idx % 64)) != 0
    }

    pub fn set(&mut self, idx: usize) {
        if idx < self.len {
            self.words[idx / 64] |= 1 << (idx % 64);
        }
    }

    pub fn clear(&mut self, idx: usize) {
        if idx < self.len {
            self.words[idx / 64] &= !(1 << (idx % 64));
        }
    }

    pub fn count(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    /// True when no bit is set.
    pub fn none(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(i, word)| {
            let mut word = *word;
            std::iter::from_fn(move || {
                if word == 0 {
                    return None;
                }
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
                Some(i * 64 + bit)
            })
        })
    }

    /// Indexes in `0..len` that are *not* set.
    pub fn complement(&self) -> Bitmap {
        let mut out = Bitmap::full(self.len);
        for (out, word) in out.words.iter_mut().zip(&self.words) {
            *out &= !word;
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Record encodings
// ---------------------------------------------------------------------------
//
// Both records start with a format byte so the layout can evolve. Lengths are
// little-endian u32.
//
//   pending  (t:)  [0x01][n u32][len u32, bytes] * n
//   finished (f:)  [0x01][n u32][done bitmap, ceil(n/8) bytes][len u32, bytes] * popcount(done)
//
// The finished record lists results in index order, one per set bit.

const FORMAT_V1: u8 = 1;

fn put_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u32).to_le_bytes());
}

fn read_u32(bytes: &[u8], at: &mut usize) -> Option<usize> {
    let end = at.checked_add(4)?;
    let value = u32::from_le_bytes(bytes.get(*at..end)?.try_into().ok()?) as usize;
    *at = end;
    Some(value)
}

fn corrupt(what: &str) -> Error {
    Error::io_error(format!("Corrupt {what} chunk record"))
}

/// Encodes a pending chunk record from its payloads.
pub fn encode_pending<'a>(payloads: impl ExactSizeIterator<Item = &'a [u8]> + Clone) -> Vec<u8> {
    let size: usize = payloads.clone().map(|payload| payload.len() + 4).sum();
    let mut out = Vec::with_capacity(5 + size);
    out.push(FORMAT_V1);
    put_u32(&mut out, payloads.len());
    for payload in payloads {
        put_u32(&mut out, payload.len());
        out.extend_from_slice(payload);
    }
    out
}

/// The payloads of one chunk: the raw record plus offsets into it, so decoding
/// is one allocation and leasing hands out slices.
#[derive(Debug, Clone, Default)]
pub struct ChunkPayloads {
    data: Vec<u8>,
    spans: Vec<(u32, u32)>,
}

impl ChunkPayloads {
    pub fn decode(data: Vec<u8>) -> Result<Self, Error> {
        let mut at = 0;
        if data.first() != Some(&FORMAT_V1) {
            return Err(corrupt("pending"));
        }
        at += 1;
        let n = read_u32(&data, &mut at).ok_or_else(|| corrupt("pending"))?;
        if n == 0 || n > MAX_CHUNK_SIZE as usize {
            return Err(corrupt("pending"));
        }
        let mut spans = Vec::with_capacity(n);
        for _ in 0..n {
            let len = read_u32(&data, &mut at).ok_or_else(|| corrupt("pending"))?;
            let end = at.checked_add(len).filter(|end| *end <= data.len());
            let end = end.ok_or_else(|| corrupt("pending"))?;
            spans.push((at as u32, len as u32));
            at = end;
        }
        Ok(Self { data, spans })
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        self.spans
            .get(idx)
            .map(|&(at, len)| &self.data[at as usize..(at + len) as usize])
    }
}

/// The results of one chunk: `results[idx]` is `Some` once task `idx` is done.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FinishedChunk {
    pub results: Vec<Option<Vec<u8>>>,
}

impl FinishedChunk {
    pub fn new(len: usize) -> Self {
        Self {
            results: vec![None; len],
        }
    }

    pub fn done(&self) -> Bitmap {
        let mut done = Bitmap::empty(self.results.len());
        for (idx, result) in self.results.iter().enumerate() {
            if result.is_some() {
                done.set(idx);
            }
        }
        done
    }

    pub fn encode(&self) -> Vec<u8> {
        let n = self.results.len();
        let size: usize = self.results.iter().flatten().map(|r| r.len() + 4).sum();
        let mut out = Vec::with_capacity(5 + n.div_ceil(8) + size);
        out.push(FORMAT_V1);
        put_u32(&mut out, n);
        let start = out.len();
        out.resize(start + n.div_ceil(8), 0);
        for (idx, result) in self.results.iter().enumerate() {
            if result.is_some() {
                out[start + idx / 8] |= 1 << (idx % 8);
            }
        }
        for result in self.results.iter().flatten() {
            put_u32(&mut out, result.len());
            out.extend_from_slice(result);
        }
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        let mut at = 0;
        if data.first() != Some(&FORMAT_V1) {
            return Err(corrupt("finished"));
        }
        at += 1;
        let n = read_u32(data, &mut at).ok_or_else(|| corrupt("finished"))?;
        if n > MAX_CHUNK_SIZE as usize {
            return Err(corrupt("finished"));
        }
        let bitmap = data
            .get(at..at + n.div_ceil(8))
            .ok_or_else(|| corrupt("finished"))?;
        at += bitmap.len();
        let mut results = vec![None; n];
        for (idx, result) in results.iter_mut().enumerate() {
            if bitmap[idx / 8] & (1 << (idx % 8)) == 0 {
                continue;
            }
            let len = read_u32(data, &mut at).ok_or_else(|| corrupt("finished"))?;
            let bytes = data.get(at..at + len).ok_or_else(|| corrupt("finished"))?;
            *result = Some(bytes.to_vec());
            at += len;
        }
        Ok(Self { results })
    }

    /// Reads only the done bitmap of an encoded record.
    pub fn decode_done(data: &[u8]) -> Result<Bitmap, Error> {
        let mut at = 0;
        if data.first() != Some(&FORMAT_V1) {
            return Err(corrupt("finished"));
        }
        at += 1;
        let n = read_u32(data, &mut at).ok_or_else(|| corrupt("finished"))?;
        let bytes = data
            .get(at..at + n.div_ceil(8))
            .ok_or_else(|| corrupt("finished"))?;
        let mut done = Bitmap::empty(n);
        for idx in 0..n {
            if bytes[idx / 8] & (1 << (idx % 8)) != 0 {
                done.set(idx);
            }
        }
        Ok(done)
    }
}

// ---------------------------------------------------------------------------
// Chunk sequence allocator
// ---------------------------------------------------------------------------

/// Meta key (default column family) holding the chunk_seq high-water mark.
pub const CHUNK_SEQ_KEY: &[u8] = b"m:chunk_seq";

/// Sequence numbers reserved per meta write.
const SEQ_BLOCK: u64 = 1 << 16;

/// Hands out strictly increasing `chunk_seq`s, server-wide (shared by all task
/// types). The high-water mark is persisted in blocks of [`SEQ_BLOCK`] *before*
/// any seq from the block is used, so a restart resumes above every seq that
/// may have been handed out and never reuses an id.
///
/// It also tracks reservations whose chunk records are not written yet:
/// [`SeqAllocator::safe_upto`] is the lowest such seq, and every pending record
/// below it is already durable. The refill cursor never moves past it, so a
/// late write can't land behind the cursor.
#[derive(Debug)]
pub struct SeqAllocator {
    inner: Mutex<SeqInner>,
}

#[derive(Debug)]
struct SeqInner {
    next: u64,
    reserved_until: u64,
    in_progress: BTreeSet<u64>,
}

impl SeqAllocator {
    /// Resumes from the persisted high-water mark (0 on a fresh store).
    pub fn open(db: &rust_rocksdb::DB) -> Result<Self, Error> {
        let stored = db
            .get(CHUNK_SEQ_KEY)
            .map_err(|err| Error::io_error(format!("Failed to read chunk_seq: {err}")))?;
        let next = match stored {
            Some(bytes) => u64::from_be_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::io_error("Corrupt chunk_seq meta record"))?,
            ),
            None => 0,
        };
        Ok(Self {
            inner: Mutex::new(SeqInner {
                next,
                reserved_until: next,
                in_progress: BTreeSet::new(),
            }),
        })
    }

    /// Reserves `count` consecutive seqs. The returned guard must be held until
    /// the chunk records are written (or the write failed).
    pub fn reserve<'a>(
        &'a self,
        db: &rust_rocksdb::DB,
        count: u64,
    ) -> Result<SeqReservation<'a>, Error> {
        let mut inner = self.lock();
        let start = inner.next;
        let end = start
            .checked_add(count)
            .ok_or_else(|| Error::io_error("chunk_seq exhausted"))?;
        if end > inner.reserved_until {
            let until = end.saturating_add(SEQ_BLOCK);
            db.put(CHUNK_SEQ_KEY, until.to_be_bytes())
                .map_err(|err| Error::io_error(format!("Failed to persist chunk_seq: {err}")))?;
            inner.reserved_until = until;
        }
        inner.next = end;
        inner.in_progress.insert(start);
        Ok(SeqReservation {
            allocator: self,
            start,
            count,
        })
    }

    /// Every pending chunk record with a seq below this is durable.
    pub fn safe_upto(&self) -> u64 {
        let inner = self.lock();
        inner.in_progress.first().copied().unwrap_or(inner.next)
    }

    /// The next seq that will be handed out.
    pub fn next(&self) -> u64 {
        self.lock().next
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SeqInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A block of reserved seqs; releases its "in progress" mark on drop.
pub struct SeqReservation<'a> {
    allocator: &'a SeqAllocator,
    pub start: u64,
    pub count: u64,
}

impl Drop for SeqReservation<'_> {
    fn drop(&mut self) {
        self.allocator.lock().in_progress.remove(&self.start);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_round_trips() {
        let id = TaskId::new(0xbeef, 0x0123_4567_89ab_cdef, 0x0042);
        let hex = id.to_string();
        assert_eq!(hex, "beef0123456789abcdef0042");
        assert_eq!(TaskId::parse(&hex).unwrap(), id);
        assert!(TaskId::parse("xyz").is_err());
        assert!(TaskId::parse("beef0123456789abcdef004g").is_err());
    }

    #[test]
    fn bitmap_ops() {
        let mut bitmap = Bitmap::full(130);
        assert_eq!(bitmap.count(), 130);
        bitmap.clear(0);
        bitmap.clear(129);
        assert_eq!(bitmap.count(), 128);
        assert!(!bitmap.get(0) && bitmap.get(1) && !bitmap.get(129) && !bitmap.get(500));
        assert_eq!(bitmap.complement().iter().collect::<Vec<_>>(), vec![0, 129]);
        assert!(Bitmap::empty(3).none());
    }

    #[test]
    fn records_round_trip() {
        let payloads: Vec<Vec<u8>> = vec![b"a".to_vec(), Vec::new(), b"ccc".to_vec()];
        let encoded = encode_pending(payloads.iter().map(Vec::as_slice));
        let decoded = ChunkPayloads::decode(encoded).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.get(2), Some(&b"ccc"[..]));
        assert_eq!(decoded.get(1), Some(&b""[..]));
        assert_eq!(decoded.get(3), None);

        let mut finished = FinishedChunk::new(10);
        finished.results[3] = Some(b"x".to_vec());
        finished.results[9] = Some(b"yz".to_vec());
        let encoded = finished.encode();
        assert_eq!(FinishedChunk::decode(&encoded).unwrap(), finished);
        assert_eq!(
            FinishedChunk::decode_done(&encoded)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![3, 9]
        );
        assert!(ChunkPayloads::decode(vec![9, 9]).is_err());
    }
}
