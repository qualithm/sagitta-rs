//! Short-lived store for intercepted query results awaiting their `DoGet`.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tracing::{debug, warn};

use crate::interceptor::{InterceptContext, QueryInterception};

/// Marks a statement handle that names a cached interception.
const HANDLE_PREFIX: &str = "\u{0}iq:";

/// Hex digits encoding the entry id and nonce in a handle.
const TOKEN_HEX_LEN: usize = 32;

/// Default time an unredeemed result is held.
const DEFAULT_TTL: Duration = Duration::from_secs(60);

/// Default maximum number of results held at once.
const DEFAULT_MAX_ENTRIES: usize = 128;

/// Default maximum Arrow memory held across all results.
const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

/// A statement handle decoded into its query and, if present, cache token.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedHandle<'a> {
  /// Cache entry id and nonce, when the handle names a cached interception.
  pub token: Option<(u64, u64)>,
  /// The SQL text the handle was created for.
  pub query: &'a str,
}

/// Decode a statement handle created by [`InterceptionCache::insert`] or a
/// plain query handle (the raw SQL text).
pub(crate) fn decode_handle(handle: &str) -> DecodedHandle<'_> {
  let parsed = handle.strip_prefix(HANDLE_PREFIX).and_then(|rest| {
    let token = rest.get(..TOKEN_HEX_LEN)?;
    let query = rest.get(TOKEN_HEX_LEN..)?.strip_prefix(':')?;
    let id = u64::from_str_radix(&token[..16], 16).ok()?;
    let nonce = u64::from_str_radix(&token[16..], 16).ok()?;
    Some(DecodedHandle {
      token: Some((id, nonce)),
      query,
    })
  });
  parsed.unwrap_or(DecodedHandle {
    token: None,
    query: handle,
  })
}

/// A result held for the caller that produced it.
struct Entry {
  nonce: u64,
  ctx: InterceptContext,
  query: String,
  interception: QueryInterception,
  bytes: usize,
  inserted: Instant,
}

#[derive(Default)]
struct State {
  entries: HashMap<u64, Entry>,
  total_bytes: usize,
  next_id: u64,
}

impl State {
  fn remove(&mut self, id: u64) -> Option<Entry> {
    let entry = self.entries.remove(&id)?;
    self.total_bytes -= entry.bytes;
    Some(entry)
  }

  fn purge_expired(&mut self, now: Instant, ttl: Duration) {
    let expired: Vec<u64> = self
      .entries
      .iter()
      .filter(|(_, e)| now.duration_since(e.inserted) >= ttl)
      .map(|(id, _)| *id)
      .collect();
    for id in expired {
      self.remove(id);
    }
  }

  fn evict_oldest(&mut self) {
    if let Some(id) = self
      .entries
      .iter()
      .min_by_key(|(_, e)| e.inserted)
      .map(|(id, _)| *id)
    {
      self.remove(id);
    }
  }
}

/// Holds results an interceptor produced at `GetFlightInfo` so `DoGet` streams
/// them instead of running the query again.
///
/// Each result is bound to the [`InterceptContext`] that produced it and is
/// handed out at most once, only to a caller with the same context. Results
/// expire after a TTL and the cache is bounded by entry count and Arrow memory;
/// a result that misses (expired, evicted, redeemed or another caller's) is
/// produced again by the interceptor under the redeeming caller's context.
pub(crate) struct InterceptionCache {
  state: Mutex<State>,
  ttl: Duration,
  max_entries: usize,
  max_bytes: usize,
}

impl Default for InterceptionCache {
  fn default() -> Self {
    Self::new(DEFAULT_TTL, DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BYTES)
  }
}

impl InterceptionCache {
  /// Create a cache with the given TTL, entry limit and byte limit.
  pub(crate) fn new(ttl: Duration, max_entries: usize, max_bytes: usize) -> Self {
    Self {
      state: Mutex::new(State::default()),
      ttl,
      max_entries,
      max_bytes,
    }
  }

  /// Hold `interception` for `ctx` and return the statement handle that
  /// redeems it, or `None` when the result is too large to hold.
  pub(crate) fn insert(
    &self,
    ctx: &InterceptContext,
    query: &str,
    interception: QueryInterception,
  ) -> Option<Bytes> {
    let bytes: usize = interception
      .batches
      .iter()
      .map(|b| b.get_array_memory_size())
      .sum();
    if self.max_entries == 0 || bytes > self.max_bytes {
      debug!(bytes, "interception_not_cached");
      return None;
    }

    let now = Instant::now();
    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
    state.purge_expired(now, self.ttl);
    while !state.entries.is_empty()
      && (state.entries.len() >= self.max_entries || state.total_bytes + bytes > self.max_bytes)
    {
      state.evict_oldest();
    }

    let id = state.next_id;
    state.next_id = state.next_id.wrapping_add(1);
    // The nonce keeps handles from being enumerated; the context binding,
    // not the nonce, is what stops another caller redeeming the result.
    let nonce = std::collections::hash_map::RandomState::new().hash_one(id);
    state.entries.insert(
      id,
      Entry {
        nonce,
        ctx: ctx.clone(),
        query: query.to_string(),
        interception,
        bytes,
        inserted: now,
      },
    );
    state.total_bytes += bytes;

    Some(Bytes::from(format!(
      "{HANDLE_PREFIX}{id:016x}{nonce:016x}:{query}"
    )))
  }

  /// Take the result named by `handle` if `ctx` produced it and it is still
  /// live. A result is handed out at most once.
  pub(crate) fn take(
    &self,
    handle: &DecodedHandle<'_>,
    ctx: &InterceptContext,
  ) -> Option<QueryInterception> {
    let (id, nonce) = handle.token?;
    let now = Instant::now();
    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
    state.purge_expired(now, self.ttl);
    let entry = state.entries.get(&id)?;
    if entry.nonce != nonce || entry.query != handle.query {
      return None;
    }
    if &entry.ctx != ctx {
      warn!(principal = ?ctx.principal, "interception_handle_caller_mismatch");
      return None;
    }
    state.remove(id).map(|e| e.interception)
  }

  /// Number of results currently held.
  #[cfg(test)]
  pub(crate) fn len(&self) -> usize {
    self.state.lock().unwrap().entries.len()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use arrow_array::{Int64Array, RecordBatch};
  use arrow_schema::{DataType, Field, Schema};

  use super::*;

  fn interception(rows: i64) -> QueryInterception {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
      schema.clone(),
      vec![Arc::new(Int64Array::from_iter_values(0..rows))],
    )
    .unwrap();
    QueryInterception {
      schema,
      batches: vec![batch],
    }
  }

  fn user(name: &str) -> InterceptContext {
    InterceptContext {
      principal: Some(name.to_string()),
      read_only: false,
    }
  }

  fn handle_str(handle: &Bytes) -> String {
    String::from_utf8(handle.to_vec()).unwrap()
  }

  #[test]
  fn test_decode_plain_handle() {
    let decoded = decode_handle("SELECT 1");
    assert_eq!(decoded.token, None);
    assert_eq!(decoded.query, "SELECT 1");
  }

  #[test]
  fn test_decode_malformed_handle_is_plain() {
    let raw = format!("{HANDLE_PREFIX}zz");
    assert_eq!(decode_handle(&raw).token, None);
    let raw = format!("{HANDLE_PREFIX}{}xSELECT 1", "0".repeat(TOKEN_HEX_LEN));
    assert_eq!(decode_handle(&raw).token, None);
    let raw = format!("{HANDLE_PREFIX}{}:SELECT 1", "g".repeat(TOKEN_HEX_LEN));
    assert_eq!(decode_handle(&raw).token, None);
  }

  #[test]
  fn test_take_once_for_same_caller() {
    let cache = InterceptionCache::default();
    let handle = cache
      .insert(&user("alice"), "SELECT 1", interception(3))
      .unwrap();
    let raw = handle_str(&handle);
    let decoded = decode_handle(&raw);
    assert_eq!(decoded.query, "SELECT 1");

    let taken = cache.take(&decoded, &user("alice")).unwrap();
    assert_eq!(taken.batches[0].num_rows(), 3);
    assert!(cache.take(&decoded, &user("alice")).is_none());
    assert_eq!(cache.len(), 0);
  }

  #[test]
  fn test_other_caller_cannot_redeem() {
    let cache = InterceptionCache::default();
    let handle = cache
      .insert(&user("alice"), "SELECT 1", interception(1))
      .unwrap();
    let raw = handle_str(&handle);
    let decoded = decode_handle(&raw);

    assert!(cache.take(&decoded, &user("mallory")).is_none());
    let mut read_only = user("alice");
    read_only.read_only = true;
    assert!(cache.take(&decoded, &read_only).is_none());
    assert!(cache.take(&decoded, &InterceptContext::default()).is_none());
    // The issuing caller still can.
    assert!(cache.take(&decoded, &user("alice")).is_some());
  }

  #[test]
  fn test_tampered_handle_misses() {
    let cache = InterceptionCache::default();
    let handle = cache
      .insert(&user("alice"), "SELECT 1", interception(1))
      .unwrap();
    let raw = handle_str(&handle);
    let decoded = decode_handle(&raw);
    let (id, nonce) = decoded.token.unwrap();

    let wrong_nonce = DecodedHandle {
      token: Some((id, nonce ^ 1)),
      query: "SELECT 1",
    };
    assert!(cache.take(&wrong_nonce, &user("alice")).is_none());
    let wrong_query = DecodedHandle {
      token: Some((id, nonce)),
      query: "SELECT 2",
    };
    assert!(cache.take(&wrong_query, &user("alice")).is_none());
    assert!(cache.take(&decoded, &user("alice")).is_some());
  }

  #[test]
  fn test_entries_expire() {
    let cache = InterceptionCache::new(Duration::ZERO, 8, usize::MAX);
    let handle = cache
      .insert(&user("alice"), "SELECT 1", interception(1))
      .unwrap();
    let raw = handle_str(&handle);
    assert!(cache.take(&decode_handle(&raw), &user("alice")).is_none());
    assert_eq!(cache.len(), 0);
  }

  #[test]
  fn test_entry_limit_evicts_oldest() {
    let cache = InterceptionCache::new(DEFAULT_TTL, 2, usize::MAX);
    let first = handle_str(&cache.insert(&user("a"), "q1", interception(1)).unwrap());
    let second = handle_str(&cache.insert(&user("a"), "q2", interception(1)).unwrap());
    let third = handle_str(&cache.insert(&user("a"), "q3", interception(1)).unwrap());
    assert_eq!(cache.len(), 2);
    assert!(cache.take(&decode_handle(&first), &user("a")).is_none());
    assert!(cache.take(&decode_handle(&second), &user("a")).is_some());
    assert!(cache.take(&decode_handle(&third), &user("a")).is_some());
  }

  #[test]
  fn test_byte_limit() {
    let one = interception(1000);
    let size: usize = one.batches.iter().map(|b| b.get_array_memory_size()).sum();
    let cache = InterceptionCache::new(DEFAULT_TTL, 8, size + size / 2);

    // Too large on its own: not held.
    assert!(
      cache
        .insert(&user("a"), "big", interception(100_000))
        .is_none()
    );

    let first = handle_str(&cache.insert(&user("a"), "q1", one).unwrap());
    let second = handle_str(&cache.insert(&user("a"), "q2", interception(1000)).unwrap());
    assert_eq!(cache.len(), 1);
    assert!(cache.take(&decode_handle(&first), &user("a")).is_none());
    assert!(cache.take(&decode_handle(&second), &user("a")).is_some());
  }

  #[test]
  fn test_zero_entry_limit_holds_nothing() {
    let cache = InterceptionCache::new(DEFAULT_TTL, 0, usize::MAX);
    assert!(cache.insert(&user("a"), "q", interception(1)).is_none());
  }
}
