package org.pbn.eventcache.test.benchmark;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;
import org.pbn.eventcache.EventCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SimpleEventCache<V extends CacheValue>
        implements EventCache<V> {
    private final ConcurrentSkipListMap<CacheKey, V> cache;

    public SimpleEventCache() {
        this.cache = new ConcurrentSkipListMap<>();
    }

    @Override
    public <K extends CacheKey> List<V> get(K from, int batchSize, boolean inclusive) {
        K to = TestOffset.of(from.offset() + batchSize);
        ConcurrentNavigableMap<CacheKey, V> eventBatch
                = cache.subMap(from, inclusive, to, true);

        return gatherEvents(eventBatch, from, batchSize);
    }

    @Override
    public void put(List<V> items) {
        for (V item : items) {
            cache.put(item.key(), item);
        }
    }

    @Override
    public <K extends CacheKey> List<V> get(K key, int batchSize) {
        return get(key, batchSize, false);
    }

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

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public long sizeInBytes() {
        return 0;
    }

    @Override
    public <K extends CacheKey> long remove(K belowThisKey) {
        ConcurrentNavigableMap<CacheKey, V> headMap = cache.headMap(belowThisKey, false);
        long removed = 0;
        if (!headMap.isEmpty()) {
            removed = headMap.size();
            headMap.clear();
        }
        return removed;
    }

    @Override
    public long remove(long olderThanThisTimestamp) {
        return 0;
    }
}
