package org.pbn.eventcache.impl;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.pbn.eventcache.impl.Container.computeIndex;

/**
 * Stores segments within this range.
 *
 * @param <V> a value that also encapsulates
 *          the corresponding cache key
 *
 * @author pbn
 */
public class Range<V extends CacheValue>
        implements Container<V> {
    private final AtomicLong lastUsed;

    private final long rangeIndex;
    private final long segmentSize;
    private final ConcurrentSkipListMap<Long, Container<V>> segments;

    /**
     * Creates a new range for the given <code>rangeIndex</code>.
     * The rangeIndex is also used as a key for this.
     *
     * @param rangeIndex index for this range.
     * @param segmentSize size of the segments contained in this.
     */
    public Range(long rangeIndex, long segmentSize) {
        this.rangeIndex = rangeIndex;
        this.segmentSize = segmentSize;
        segments = new ConcurrentSkipListMap<>();
        lastUsed = new AtomicLong(System.currentTimeMillis());
    }

    public long lastUsed() {
        return lastUsed.get();
    }

    public void put(List<V> items) {
        if (items.isEmpty()) {
            throw new IllegalArgumentException("Cannot put empty list of items");
        }

        touch();

        CacheKey firstKey = items.getFirst().key();
        CacheKey lastKey = items.getLast().key();
        long segmentIndexFirstKey = computeIndex(firstKey.offset(), segmentSize);
        long segmentIndexLastKey = computeIndex(lastKey.offset(), segmentSize);
        if (segmentIndexFirstKey != segmentIndexLastKey) {
            for (V item : items) {
                put(item.key(), item);
            }
        } else {
            // Optimization: assumes all items belong to a segment if
            // the indexes of first and last elements are equal
            segments.compute(segmentIndexFirstKey, (k, v) -> {
                Container<V> segment = v;
                if (v == null) {
                    segment = new Segment<>(k, segmentSize);
                }

                segment.put(items);

                return segment;
            });
        }
    }

    public <K extends CacheKey> void put(K k, V v) {
        long segmentIndex = computeIndex(k.offset(), segmentSize);
        Container<V> segment = segments.computeIfAbsent(segmentIndex, this::apply);
        segment.put(k, v);
    }

    public <K extends CacheKey> List<V> get(K from, int maxBatchSize, boolean inclusive) {
        touch();

        List<V> result = readFromSegment(from, maxBatchSize, inclusive);
        List<V> more = result;
        while (!more.isEmpty() && result.size() < maxBatchSize) {
            int newBatchSize = maxBatchSize - result.size();
            K newFrom = result.getLast().key();
            more = readFromSegment(newFrom, newBatchSize, false);
            if (!more.isEmpty()) {
                result.addAll(more);
            }
        }

        return result;
    }

    private <K extends CacheKey> List<V> readFromSegment(K from, int batchSize, boolean inclusive) {
        long segmentIndex = computeIndex(from.offset(), segmentSize);
        Container<V> segment = segments.get(segmentIndex);

        if (!inclusive && (segment == null || segment.isLast(from))) {
            segment = segments.get(segmentIndex + 1);
        }

        List<V> items = Collections.emptyList();
        if (segment != null) {
            items = segment.get(from, batchSize, inclusive);
        }

        return items;
    }

    public <K extends CacheKey> long remove(K belowThisKey /* exclusive */) {
        Long segmentIndex = computeIndex(belowThisKey.offset(), segmentSize);

        return Container.remove(segments, belowThisKey, segmentIndex);
    }

    public long remove(long olderThanThisTimestamp) {
        return Container.remove(segments, olderThanThisTimestamp);
    }

    public long size() {
        return segments.values()
                .stream()
                .mapToLong(Container::size)
                .sum();
    }

    public long sizeInBytes() {
        return segments.values()
                .parallelStream()
                .mapToLong(Container::sizeInBytes)
                .sum();
    }

    public int segmentsSize() {
        return segments.size();
    }

    public long index() {
        return rangeIndex;
    }

    public <K extends CacheKey> boolean isLast(K from) {
        Long lastSegmentKey = segments.lastKey();
        return segments.get(lastSegmentKey)
                .isLast(from);
    }

    @Override
    public String toString() {
        return "Range{" +
                "rangeIndex=" + rangeIndex +
                ", segmentSize=" + segmentSize +
                ", segmentsCount=" + segments.size() +
                '}';
    }

    public String deepToString() {
        return "r[" + rangeIndex +
                "]=[nos=" +
                segments.size() +
                "], {" +
                segments.values()
                        .stream()
                        .sorted(new ReverseComparator<>())
                        .map(Container::deepToString)
                        .collect(Collectors.joining(", ")) +
                "}";
    }

    private Segment<V> apply(long index) {
        return new Segment<>(index, segmentSize);
    }

    private void touch() {
        lastUsed.updateAndGet(prev -> Math.max(prev, System.currentTimeMillis()));
    }
}
