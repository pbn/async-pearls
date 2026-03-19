package org.pbn.eventcache.impl;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public interface Container<V extends CacheValue> {
    <K extends CacheKey> List<V> get(K from, int maxBatchSize, boolean inclusive);

    <K extends CacheKey> boolean isLast(K from);

    <K extends CacheKey> void put(K k, V v);
    void put(List<V> items);

    <K extends CacheKey> long remove(K belowThisKey /* exclusive */);
    long remove(long olderThanThisTimestamp);

    long size();
    long sizeInBytes();

    long lastUsed();

    long index();
    String deepToString();

    class ReverseComparator<R extends Container<?>>
            implements Comparator<R> {
        @Override
        public int compare(R o1, R o2) {
            return Long.compare(o2.index(), o1.index());
        }
    }

    /**
     * Returns a location (index) of a bucket/container given the
     * offset aware cache key and the bucket size. Note: the index is
     * for a bucket/container within a collection of containers, not the
     * element index within a container.
     *
     * @param offset an offset
     * @param containerSize size of a bucket
     * @return index of the bucket/container that contains the given
     * cache key.
     * @throws IllegalArgumentException if the given offset
     * is a negative number.
     */
    static long computeIndex(Long offset, Long containerSize) {
        if (offset < 0) {
            throw new IllegalArgumentException("Negative offset number is not accepted");
        }

        return (long)Math.floor((double) (offset - 1) / containerSize) + 1;
    }

    static <K extends CacheKey, V extends CacheValue> Long remove(ConcurrentSkipListMap<Long, Container<V>> elements,
                                            K belowThisKey,
                                            Long index) {
        // Remove all containers less than the index supplied above
        ConcurrentNavigableMap<Long, Container<V>> headMap = elements.headMap(index, false);
        long itemsRemoved = 0;
        if (!headMap.isEmpty()) {
            itemsRemoved = headMap.values()
                    .parallelStream()
                    .mapToLong(Container::size)
                    .sum();

            headMap.clear();
        }

        AtomicLong itemsRemovedFromContainers = new AtomicLong(0);

        // Remove items within this container/element that hold items belowThisKey
        elements.computeIfPresent(index, (_, v) -> {
            itemsRemovedFromContainers.set(v.remove(belowThisKey));
            if (v.size() == 0) {
                return null;
            } else {
                return v;
            }
        });

        return itemsRemoved + itemsRemovedFromContainers.get();
    }

    static <V extends CacheValue> long remove(ConcurrentSkipListMap<Long, Container<V>> elements,
                       long olderThanThisTimestamp) {
        // Remove containers/elements that are idle
        List<Long> containerIndexes = new ArrayList<>();

        for (Long index : elements.keySet()) {
            if (elements.get(index).lastUsed() < olderThanThisTimestamp) {
                containerIndexes.add(index);
            }
        }

        AtomicLong removed = new AtomicLong(0L);

        for (Long index : containerIndexes) {
            elements.computeIfPresent(index, (k, v) -> {
                if (v.lastUsed() < olderThanThisTimestamp) {
                    removed.addAndGet(v.size());
                    return null;
                } else {
                    return v;
                }
            });
        }

        // Items within a container (element) that are idle
        long itemsRemoved = elements.values()
                .parallelStream()
                .mapToLong(r -> r.remove(olderThanThisTimestamp))
                .sum();

        removed.addAndGet(itemsRemoved);

        return removed.get();
    }
}
