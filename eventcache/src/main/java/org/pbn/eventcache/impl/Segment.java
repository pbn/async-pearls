package org.pbn.eventcache.impl;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.pbn.eventcache.impl.Container.computeIndex;

/**
 * Stores items in sorted order based on the offset value.
 * There can be a gap in the sequence due to unread events
 * from the source, or there can be a gap in the stored
 * stream itself due to network issues, etc.
 * <p/>
 * The {@code valueMap} is an unbounded {@code ConcurrentSkipListMap},
 * as the capacity limits the number of items.
 *
 * @param <V> value related to an offset, for example,
 *           a Kafka record.
 *
 * @author pbn
 */
public class Segment<V extends CacheValue> implements Container<V> {

    // Inputs
    private final long segmentIndex;
    private final long capacity;

    // State
    private final AtomicLong sizeInBytes = new AtomicLong(0L);
    private final AtomicLong lastUsed;

    /**
     * Holds the sorted list of items based on their offset value.
     */
    private final ConcurrentSkipListMap<CacheKey, V> valueMap;

    /**
     * Creates this with the given capacity.
     *
     * @param capacity a size for this.
     */
    public Segment(long segmentIndex, long capacity) {
       this.segmentIndex = segmentIndex;
        this.capacity = capacity;
        Comparator<CacheKey> offsetComparator = Comparator.comparing(CacheKey::offset);
        valueMap = new ConcurrentSkipListMap<>(offsetComparator);
        lastUsed = new AtomicLong(System.currentTimeMillis());
    }

    public long lastUsed() {
        return lastUsed.get();
    }

    /**
     * Stores the given value corresponding to the given key.
     *
     * @param k a key
     * @param v a value
     * @throws IllegalArgumentException if the item's offset
     * doesn't belong to this segment.
     */
    public <K extends CacheKey> void put(K k, V v) throws IllegalStateException {
        long itemIndex = computeIndex(k.offset(), capacity);
        if (segmentIndex != itemIndex) { // pre-condition
            throw new IllegalArgumentException("Segment index mismatch: " + segmentIndex + " != " + itemIndex);
        }

        if (valueMap.putIfAbsent(k, v) == null) {
            sizeInBytes.addAndGet(v.eventSize());
        }
    }

    public void put(List<V> items) throws IllegalStateException {
        touch();

        for (V item : items) {
            put(item.key(), item);
        }
    }

    public <K extends CacheKey> List<V> get(K from /* exclusive */, int batchSize) {
        return get(from, batchSize, false);
    }

    public <K extends CacheKey> List<V> get(K from, int batchSize, boolean inclusive) {
        touch();

        // Get sorted items from the given key.
        ConcurrentNavigableMap<CacheKey, V> subMap = valueMap.tailMap(from, inclusive);

        return gatherEvents(subMap, from, batchSize);
    }

    public <K extends CacheKey> long remove(K belowThisKey) {
        ConcurrentNavigableMap<CacheKey, V> headMap = valueMap.headMap(belowThisKey, false);
        int removed = 0;

        if (!headMap.isEmpty()) {
            long removedBytes = headMap.values().stream().mapToLong(CacheValue::eventSize).sum();
            sizeInBytes.addAndGet(-removedBytes);

            removed = headMap.size();
            headMap.clear();
        }

        return removed;
    }

    @Override
    public long remove(long olderThanThisTimestamp) {
        // The individual items are not tracked for
        // idle time, so this is a no-op
        return 0L;
    }

    /**
     * Gathers items that are contiguous in the given sub-map.
     */
    private static <K extends CacheKey, V extends CacheValue>
    List<V> gatherEvents(ConcurrentNavigableMap<K, V> subMap,
                         K prevKey,
                         int batchSize) {
        List<V> items = Collections.emptyList();

        if (!subMap.isEmpty()) {
            items = new ArrayList<>(batchSize);
            int gatherCount = 0;
            long prev = prevKey.offset();
            for (Map.Entry<K, V> entry : subMap.entrySet()) {
                long current = entry.getKey().offset();
                // Check for the n+1 item, if there is a gap,
                // stop gathering and return the list.
                if (gatherCount < batchSize && ((prev == current)
                        || prev == (current - 1))) {
                    items.add(entry.getValue());
                    gatherCount++;
                    prev = current;
                } else {
                    break;
                }
            }
        }

        return items;
    }

    public long size() {
        return valueMap.size();
    }

    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    public boolean isEmpty() {
        return valueMap.isEmpty();
    }

    public <K extends CacheKey> boolean isLast(K offset) {
        return !valueMap.isEmpty() && valueMap.lastKey().equals(offset);
    }

    public long index() {
        return segmentIndex;
    }

    @Override
    public String toString() {
        return "Segment{" +
                ", index=" + segmentIndex +
                ", capacity=" + capacity +
                ", valueMap=" + valueMap.size() +
                '}';
    }

    public String deepToString() {
        return "s[" + segmentIndex + "]="
                + "[" + valueMap.lastKey().offset()
                + "..."+ valueMap.firstKey().offset()
                + "]";
    }

    private void touch() {
        lastUsed.updateAndGet(prev -> Math.max(prev, System.currentTimeMillis()));
    }
}
