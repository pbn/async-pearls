package org.pbn.eventcache.impl;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;
import org.pbn.eventcache.EventCache;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static org.pbn.eventcache.impl.Container.computeIndex;

/**
 * Stores ranges of items, each range contains segments.
 * Each segment contains items.
 *
 * @param <V> the value type stored in the cache.
 *
 * @author pbn
 */
public class ConcurrentEventCache<V extends CacheValue>
        implements EventCache<V> {

    private static final Duration DEFAULT_TTL_10_MINUTES = Duration.ofMinutes(10);

    private final long rangeSize;
    private final long segmentSize;
    private final Duration ttl;

    private final ConcurrentSkipListMap<Long, Container<V>> ranges;

    /**
     * Creates a range cache instance with the given range size
     * and segment size.
     * The rangeSize >= segmentSize, for example,
     * rangeSize could be 100 K and segmentSize could be 2 K.
     *
     * @param rangeSize number of items per range
     * @param segmentSize number of items per segment
     */
    public ConcurrentEventCache(long rangeSize, long segmentSize) {
        this.rangeSize = rangeSize;
        this.segmentSize = segmentSize;
        ttl = DEFAULT_TTL_10_MINUTES;
        ranges = new ConcurrentSkipListMap<>();
    }

    public ConcurrentEventCache(long rangeSize, long segmentSize, Duration ttl) {
        this.rangeSize = rangeSize;
        this.segmentSize = segmentSize;
        this.ttl = ttl;
        ranges = new ConcurrentSkipListMap<>();
    }

    public Duration getTtl() {
        return ttl;
    }

    private <K extends CacheKey> void put(K k, V v) {
        long rangeIndex = computeIndex(k.offset(), rangeSize);
        Container<V> range = ranges.computeIfAbsent(rangeIndex,
                _ -> new Range<>(rangeIndex, segmentSize));
        range.put(k, v);
    }

    /**
     * Stores items based on their offset. The items should be sorted.
     *
     * @param items a list of items to cache
     * @throws IllegalStateException if the segment
     *                               is at capacity.
     */
    @Override
    public void put(List<V> items) throws IllegalStateException {
        V first = items.getFirst();
        Long firstIndex = computeIndex(first.key().offset(), rangeSize);
        V last = items.getLast();
        Long lastIndex = computeIndex(last.key().offset(), rangeSize);

        if (firstIndex.equals(lastIndex)) { /* all items are in the same range */
            Container<V> range = ranges.computeIfAbsent(firstIndex,
                    _ -> new Range<>(firstIndex, segmentSize));
            range.put(items);
        } else {
            for (V item : items) {
                put(item.key(), item);
            }
        }
    }

    @Override
    public <K extends CacheKey> List<V> get(K from, int maxBatchSize) {
        return get(from, maxBatchSize, false);
    }

    /**
     * Returns a list of items up to the given {@code maxBatchSize},
     * the list doesn't contain any gaps.
     *
     * @param from a starting item's offset
     * @param maxBatchSize the max number of items to return.
     * @param inclusive whether item keyed by {@code from} should
     *                  be included in the list or not.
     * @return a list of items up to the maxBatchSize. An empty
     * list indicates there isn't an item keyed by {@code from}
     * when {@code inclusive} is true, or 2) there isn't an item
     * keyed by {@code from + 1} when {@code inclusive} is false.
     */
    @Override
    public <K extends CacheKey> List<V> get(K from, int maxBatchSize, boolean inclusive) {
        List<V> result = readFromRange(from, maxBatchSize, inclusive);
        List<V> more = result;
        while (!more.isEmpty() && result.size() < maxBatchSize) {
            int newBatchSize = maxBatchSize - result.size();
            K newFrom = result.getLast().key();
            more = readFromRange(newFrom, newBatchSize, false);
            if (!more.isEmpty()) {
                result.addAll(more);
            }
        }

        return result;
    }

    protected ConcurrentMap<Long, Container<V>> ranges() {
        return ranges;
    }

    private <K extends CacheKey> List<V> readFromRange(K from, int maxBatchSize, boolean inclusive) {
        long rangeIndex = computeIndex(from.offset(), rangeSize);
        Container<V> range = ranges.get(rangeIndex);
        if (!inclusive && (range == null /* when the "from" key is 0L and the cache contains events from 1L */
                || range.isLast(from) /* when the "from" key is the last offset of a range */)) {
            range = ranges.get(rangeIndex + 1);
        }

        if (range != null) {
            return range.get(from, maxBatchSize, inclusive);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public long size() {
        return ranges.values().stream().mapToLong(Container::size).sum();
    }

    @Override
    public long sizeInBytes() {
        return ranges.values().parallelStream().mapToLong(Container::sizeInBytes).sum();
    }

    /**
     * Removes items below the given key {@code belowThisKey}
     *
     * @param belowThisKey an offset key.
     * @return number of items removed.
     */
    @Override
    public <K extends CacheKey> long remove(K belowThisKey /* exclusive */) {
        Long rangeIndex = computeIndex(belowThisKey.offset(), rangeSize);

        return Container.remove(ranges, belowThisKey, rangeIndex);
    }

    @Override
    public long remove(long olderThanThisTimestamp) {
        return Container.remove(ranges, olderThanThisTimestamp);
    }

    public String deepToString() {
        return "rcap=" + rangeSize +
                ", scap=" + segmentSize +
                ", nor=" + ranges.size() +
                System.lineSeparator() +
                "{" +
                ranges.values()
                        .stream()
                        .sorted(new Container.ReverseComparator<>())
                        .map(Container::deepToString)
                        .collect(Collectors.joining(System.lineSeparator())) +
                "}";
    }
}
