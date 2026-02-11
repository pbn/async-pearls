# EventCache Module — Design Document

## Overview

The **eventcache** module provides an in-memory cache for *offset-addressable* values (“events”). Events are ordered by a `CacheKey` (typically containing a monotonically increasing `offset`) and are stored so callers can:

- **Read** a bounded batch starting at (or after) a given key
- **Insert** events (often in order, often in batches)
- **Evict** events strictly below a cutoff key (retention / compaction)

The primary implementation is **`ConcurrentEventCache`**, optimized for concurrent access and for working with large offset ranges efficiently.

---

## Key Concepts

### Event, Key, Value

- **`CacheKey`**: identifies an event. Most importantly it exposes an `offset()` (a `long`) and defines ordering (e.g., via `Comparable`).
- **`CacheValue`**: a stored value that can return its key (e.g., `value.key()`).
- An “event stream” is a sequence of values ordered by key/offset.

### Ordering & “gapless” reads

A central semantic of this module is that reads are intended to return **contiguous sequences** of events (no missing offsets inside the returned list).

- If the cache cannot provide a contiguous sequence from the requested starting point (depending on `inclusive`), the result may be empty or shorter than requested.

This is particularly useful when the consumer needs to replay a stream in order and cannot skip offsets.

---

## Public API

### `EventCache<V extends CacheValue>`

Core operations:

1. **Read**
    - `get(key, batchSize, inclusive)`
    - `get(key, batchSize)` (convenience overload; inclusion behavior is implementation-defined)

2. **Write**
    - `put(List<V> events)`

3. **Metrics**
    - `size()`

4. **Eviction**
    - `remove(belowThisKey)` removes entries strictly less than the cutoff key

#### Behavioral notes

- `get(...)` returns *at most* `batchSize` elements.
- `put(...)` may reorder/deduplicate/overwrite as required by the implementation’s invariants.
- `remove(...)` is exclusive: keys strictly less than `belowThisKey` are removed.

---

## ConcurrentEventCache Design

### Goals

- **Concurrency-friendly**: support multiple readers/writers without global locks.
- **Efficient retention**: fast removal of old data below a cutoff.
- **Efficient reads**: read batches without scanning the whole cache.
- **Scalable memory layout**: avoid monolithic structures for large keyspaces.

### High-level structure: Ranges → Segments → Items

`ConcurrentEventCache` partitions the offset space into **ranges**, and each range into **segments**.

- **Range**
    - Covers a contiguous interval of offsets of size `rangeSize` (e.g., 100k offsets per range).
    - Addressed by `rangeIndex = offset / rangeSize` (conceptually).
- **Segment**
    - Subdivides a range into smaller chunks of size `segmentSize` (e.g., 2k offsets per segment).
    - Primarily reduces contention and improves locality for writes/reads within a large range.

This creates a 3-level layout:

1. `ConcurrentSkipListMap<rangeIndex, Range<V>>` (ordered, concurrent)
2. `Range` internally manages multiple `Segment`s (details internal to the module)
3. `Segment` stores the actual offset→value mappings for that segment

### Why `ConcurrentSkipListMap` for ranges?

Using an ordered, concurrent navigable map enables:

- Efficient lookup of the range for a given offset
- Efficient traversal to the “next” range when reads cross boundaries
- Efficient eviction of **all ranges below a cutoff range** via `headMap(...).clear()`

### Read algorithm (conceptual)

A `get(from, maxBatchSize, inclusive)` behaves like:

1. Determine which range contains `from.offset`.
2. Read as many contiguous items as possible from that range (respecting `inclusive`).
3. If the batch is not full and the last returned item is at the end of a range, continue into the next range(s), always requiring contiguity.
4. Stop when:
    - `maxBatchSize` is reached, or
    - the next expected offset is not present (gap), or
    - there are no more ranges/items

This yields *gapless* batches, even when the batch spans multiple ranges.

### Write algorithm (conceptual)

`put(items)` assumes the list is already sorted by offset (recommended by design):

- If all items belong to the same range, insert them as a batch into that range (more efficient).
- Otherwise, insert item-by-item across ranges.

Internally, ranges are created on demand (`computeIfAbsent`).

### Eviction algorithm (conceptual)

`remove(belowThisKey)`:

1. Compute the cutoff range index from the key’s offset.
2. Remove **entire ranges** with `rangeIndex < cutoffRangeIndex` in bulk.
3. For the cutoff range itself, remove only the entries below the cutoff key (segment-level deletion).
4. Drop the cutoff range entirely if it becomes empty.

### Concurrency model & guarantees (practical)

- The top-level range index is concurrent and ordered.
- Range creation is atomic per range index (via `computeIfAbsent`).
- Reads and writes may interleave; results reflect a “best effort” snapshot consistent with concurrent collections (not a transactional snapshot).
- The design favors:
    - High throughput
    - Non-blocking reads
    - Efficient bulk eviction
- Exact overwrite/dedup semantics (same key inserted twice) depend on lower-level `Range/Segment` policy.

---

## Configuration

### `rangeSize` and `segmentSize`

You choose sizes at construction time:

- `rangeSize` must be **≥** `segmentSize`
- Typical intent:
    - `rangeSize`: large enough to make range map small (fewer ranges)
    - `segmentSize`: small enough to reduce contention and bound per-segment work

Rules of thumb:

- If you expect frequent eviction (sliding window), a moderately sized `rangeSize` helps eviction be coarse-grained and fast.
- If you expect heavy concurrent writers around nearby offsets, a smaller `segmentSize` can reduce hotspots.

---

## Example Usage

Below is an illustrative usage example showing the intended access patterns. (The concrete `CacheKey`/`CacheValue` types are project-specific; the example provides minimal implementations.)

```java
import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;
import org.pbn.eventcache.ConcurrentEventCache;
import org.pbn.eventcache.EventCache;

import java.util.List;

public class EventCacheExample {

    // Minimal key/value for demonstration.
    static final class OffsetKey implements CacheKey {
        private final long offset;

        OffsetKey(long offset) {
            this.offset = offset;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public int compareTo(CacheKey other) {
            return Long.compare(this.offset, other.offset());
        }
    }

    static final class MyEvent implements CacheValue {
        private final OffsetKey key;
        private final String payload;

        MyEvent(long offset, String payload) {
            this.key = new OffsetKey(offset);
            this.payload = payload;
        }

        @Override
        public OffsetKey key() {
            return key;
        }

        public String payload() {
            return payload;
        }
    }

    public static void main(String[] args) {
        // Choose partitioning parameters appropriate for your workload.
        EventCache<MyEvent> cache = new ConcurrentEventCache<>(100_000L, 2_000L);

        // Put a sorted batch (recommended).
        cache.put(List.of(
                new MyEvent(1, "a"),
                new MyEvent(2, "b"),
                new MyEvent(3, "c")
        ));

        // Read starting AFTER offset=1 (inclusive=false means start at next offset).
        List<MyEvent> batch = cache.get(new OffsetKey(1), 10, false);
        // expected: offsets [2, 3] (gapless, up to batch size)

        // Read starting AT offset=2 (inclusive=true includes offset=2 if present).
        List<MyEvent> batch2 = cache.get(new OffsetKey(2), 10, true);
        // expected: offsets [2, 3]

        // Evict everything below offset=3 (removes offsets 1 and 2).
        long removed = cache.remove(new OffsetKey(3));

        long remaining = cache.size();
    }
}
```


### Example: Gapless behavior

If you insert offsets `1, 2, 4` (missing `3`), then:

- `get(from=1, batchSize=10, inclusive=true)` should return `[1, 2]` (stops before the gap at 3→4).
- `get(from=2, batchSize=10, inclusive=false)` should attempt to start at `3` and likely return `[]` because `3` is missing.

(This behavior is what makes the cache suitable for ordered replay without skipping.)

---

## Non-goals / Out of scope

- Durable persistence (this is in-memory)
- Transactional snapshots across concurrent writers
- Arbitrary secondary indexing beyond offset-based access

---

## Summary

`ConcurrentEventCache` is a concurrent, partitioned, offset-addressable cache designed for:

- Ordered, gapless batch reads
- Efficient batch inserts (especially when pre-sorted)
- Fast retention via bulk removal of entire ranges and partial removal within a range
