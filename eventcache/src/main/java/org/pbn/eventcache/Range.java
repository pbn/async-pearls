package org.pbn.eventcache;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Stores segments within this range.
 *
 * @param <V> a value that also encapsulates
 *          the corresponding cache key
 *
 * @author pbn
 */
public class Range<V extends CacheValue>
        implements Indexable {
    private final long rangeIndex;
    private final long segmentSize;
    private final ConcurrentSkipListMap<Long, Segment<V>> segments;

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
    }

    public void put(CacheKey k, V v) {
        long segmentIndex = getIndex(k.offset(), segmentSize);
        Segment<V> segment = segments.computeIfAbsent(segmentIndex, this::apply);
        segment.put(k, v);
    }

    public boolean put(List<V> items) {
        for (V item : items) {
            put(item.key(), item);
        }

        return true;
    }

    public <K extends CacheKey> List<V> get(K from, int maxBatchSize, boolean inclusive) {
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
        long segmentIndex = getIndex(from.offset(), segmentSize);
        Segment<V> segment = segments.get(segmentIndex);

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
        Long segmentIndex = getIndex(belowThisKey.offset(), segmentSize);

        // Remove all segments less than this key
        ConcurrentNavigableMap<Long, Segment<V>> headMap = segments.headMap(segmentIndex, false);
        long removed = headMap.values()
                .stream()
                .mapToLong(Segment::size)
                .sum();

        headMap.clear();

        // Remove items from within the segment
        AtomicLong itemsRemovedFromSegment = new AtomicLong(0);

        // Remove segments within the range
        segments.computeIfPresent(segmentIndex, (_, v) -> {
            itemsRemovedFromSegment.set(v.remove(belowThisKey));
            if (v.isEmpty()) {
                return null;
            } else {
                return v;
            }
        });

        return removed + itemsRemovedFromSegment.get();
    }

    public long size() {
        return segments.values()
                .stream()
                .mapToLong(Segment::size)
                .sum();
    }

    public int segmentsSize() {
        return segments.size();
    }

    public long getRangeIndex() {
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
                        .sorted(new ReverseSegmentComparator<Segment<?>>())
                        .map(Segment::deepToString)
                        .collect(Collectors.joining(", ")) +
                "}";
    }

    private Segment<V> apply(long index) {
        return new Segment<>(index, segmentSize);
    }

    private static class ReverseSegmentComparator<S extends Segment<?>>
            implements Comparator<S> {
        @Override
        public int compare(S o1, S o2) {
            return Long.compare(o2.getSegmentIndex(), o1.getSegmentIndex());
        }
    }
}
