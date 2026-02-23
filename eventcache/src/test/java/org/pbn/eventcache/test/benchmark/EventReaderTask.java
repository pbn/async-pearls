package org.pbn.eventcache.test.benchmark;


import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;
import org.pbn.eventcache.EventCache;
import org.pbn.eventcache.impl.ConcurrentEventCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class EventReaderTask<K extends CacheKey, V extends CacheValue>
        implements Runnable {
    private final long startOffset;
    private final long endOffset;
    private final int batchSize;
    private final int expectedEvents;

    private final EventSource<V> eventSource;
    private final EventCache<V> eventCache;

    public EventReaderTask(long startOffset,
                           long endOffset,
                           int batchSize,
                           int expectedEvents,
                           EventSource<V> eventSource,
                           EventCache<V> eventCache) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.batchSize = batchSize;
        this.expectedEvents = expectedEvents;
        this.eventSource = eventSource;
        this.eventCache = eventCache;
    }

    private final List<V> events = new ArrayList<>();
    private final AtomicLong readFromCache = new AtomicLong(0);
    private final AtomicLong readFromSource = new AtomicLong(0);

    private final AtomicReference<K> lastKey = new AtomicReference<>(null);

    public List<V> getEvents() {
        if (events.size() > expectedEvents) {
            System.out.println(this);
            if (eventCache instanceof ConcurrentEventCache<V>) {
                System.out.println(((ConcurrentEventCache<V>)eventCache).deepToString());
            }
        }

        return events;
    }

    public K getLastReadKey() {
        return lastKey.get();
    }

    public AtomicLong getReadFromCache() {
        return readFromCache;
    }

    public AtomicLong getReadFromSource() {
        return readFromSource;
    }

    @Override
    public void run() {
        K startingKey = TestOffset.of(startOffset);
        boolean inclusive = true;
        do {
            if (events.size() >= expectedEvents) {
                System.out.println("Next iteration:" + startingKey.offset() + "," + endOffset);
            }

            List<V> fromCache = eventCache.get(startingKey, batchSize, inclusive);
            if (fromCache.isEmpty()) {
                int bsize = batchSize;
                if ((endOffset - startingKey.offset()) < batchSize) {
                    bsize = (int)(endOffset - startingKey.offset());
                }
                List<V> fromSource = eventSource.get(startingKey, bsize, inclusive);
                eventCache.put(fromSource);
                events.addAll(fromSource);
                readFromSource.addAndGet(fromSource.size());
                startingKey = fromSource.getLast().key();
            } else {
                long so = startingKey.offset();
                long cacheSo = fromCache.getFirst().key().offset();
                if (!(cacheSo == so + 1 || cacheSo == so)) {
                    throw new IllegalStateException("Cache is corrupted!" + ", cacheSo=" + cacheSo + ", so=" + so);
                }
                events.addAll(fromCache);
                readFromCache.addAndGet(fromCache.size());
                startingKey = fromCache.getLast().key();
            }

            lastKey.set(startingKey);

            inclusive = false;

        } while (startingKey.offset() < endOffset);
    }

    @Override
    public String toString() {
        return "EventReaderTask{" +
                "startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", batchSize=" + batchSize +
                ", readFromCache=" + readFromCache +
                ", readFromSource=" + readFromSource +
                ", events=" + events.size() +
                ", validation=" + validate() +
                '}';
    }

    private boolean validate() {
        for (int i = 0; i < events.size() -1; i++) {
            long o1 = events.get(i).key().offset();
            long o2 = events.get(i+1).key().offset();
            if (o2 - o1 != 1) {
                System.out.println("Found gaps/duplicates..." + o1 + ", " + o2);
                return false;
            }
        }

        return true;
    }
}
