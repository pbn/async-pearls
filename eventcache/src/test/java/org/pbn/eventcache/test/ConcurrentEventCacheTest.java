package org.pbn.eventcache.test;

import org.junit.jupiter.api.Test;
import org.pbn.eventcache.ConcurrentEventCache;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code ConcurrentEventCache}
 *
 * @author pbn
 */
public class ConcurrentEventCacheTest {

    /**
     * Tests get events with various numEvents numbers.
     * 1. numEvents that span two segments.
     * 2. numEvents that span two ranges.
     */
    @Test
    void getEventsWithDifferentSpans() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        List<TestEvent<TestOffset>> events = generateEvents(1L, 10100);
        cache.put(events);

        // Act - test segment boundaries
        // batchSizeSegmentSpan spans 2 segments
        int batchSizeSegmentSpan = 150;
        List<TestEvent<TestOffset>> eventsRead
                = cache.get(TestOffset.of(0L), batchSizeSegmentSpan, false);
        // Assert
        assertEquals(batchSizeSegmentSpan, eventsRead.size());

        // Act - test range boundaries
        // batchSizeRangeSpan spans 2 ranges
        int batchSizeRangeSpan = 1050;
        eventsRead = cache.get(TestOffset.of(0L), batchSizeRangeSpan, false);

        // Assert
        assertEquals(batchSizeRangeSpan, eventsRead.size());
    }

    /**
     * Puts a series of events with gaps and tests the
     * following.
     * 1. Removing events before the gap.
     * 2. Removing events after the gap with the events
     *    present after the gap.
     */
    @Test
    void removeEventsWithGaps() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        int eventsCount = 5050;

        List<TestEvent<TestOffset>> events = generateEvents(1L, eventsCount);
        cache.put(events);

        events = generateEvents(6000L, eventsCount);
        cache.put(events);

        // Act - Remove events before the gap
        long removed = cache.remove(TestOffset.of(6000L));

        // Assert
        assertEquals(5050, cache.size());
        assertEquals(5050, removed);

        // Act - Remove events after the gap
        removed = cache.remove(TestOffset.of(11050L));
        assertEquals(0, cache.size());
        assertEquals(5050, removed);
    }

    /**
     * Puts a series of events with gaps and tests the
     * following.
     * Gets events with various numEvents numbers.
     * 1. numEvents that span two segments.
     * 2. numEvents that span two ranges.
     */
    @Test
    void getEventsWithDifferentSpansWithGaps() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        int eventsCount = 5050;
        List<TestEvent<TestOffset>> eventsBeforeGap;
        List<TestEvent<TestOffset>> eventsAfterGap;
        List<TestEvent<TestOffset>> events = generateEvents(1L, eventsCount);
        eventsBeforeGap = new ArrayList<>(events);
        cache.put(events);
        events = generateEvents(6000L, eventsCount);
        eventsAfterGap = new ArrayList<>(events);
        cache.put(events);

        // Act - test segment boundaries with both inclusive and exclusive
        // offsets.
        // batchSizeSegmentSpan spans 2 segments
        int batchSizeSegmentSpan = 150;
        List<TestEvent<TestOffset>> eventsReadExclusive
                = cache.get(TestOffset.of(5000), batchSizeSegmentSpan, false);
        List<TestEvent<TestOffset>> eventsReadInclusive
                = cache.get(TestOffset.of(5000), batchSizeSegmentSpan, true);
        // Assert
        assertEquals(50, eventsReadExclusive.size());
        assertEquals(51, eventsReadInclusive.size());
        assertEquals(TestOffset.of(5000), eventsReadInclusive.getFirst().key());


        // Act - test range boundaries
        // batchSizeRangeSpan spans 2 ranges
        int batchSizeRangeSpan = 150;
        eventsReadExclusive = cache.get(TestOffset.of(6000), batchSizeRangeSpan, false);
        eventsReadInclusive = cache.get(TestOffset.of(6000), batchSizeRangeSpan, true);

        // Assert
        assertEquals(batchSizeRangeSpan, eventsReadExclusive.size());
        assertEquals(batchSizeRangeSpan, eventsReadInclusive.size());
        assertEquals(TestOffset.of(6000), eventsReadInclusive.getFirst().key());

        // Read sequentially with different batch sizes
        // With 100 batch size
        int batchSize = 100;
        List<TestEvent<TestOffset>> eventsRead = getEvents(cache, TestOffset.of(0L), batchSize);
        assertEquals(eventsBeforeGap, eventsRead);

        batchSize = 150;
        eventsRead = getEvents(cache, TestOffset.of(0L), batchSize);
        assertEquals(eventsBeforeGap, eventsRead);

        batchSize = 1001;
        eventsRead = getEvents(cache, TestOffset.of(0L), batchSize);
        assertEquals(eventsBeforeGap, eventsRead);

        // Read events after the gap
        eventsRead = getEvents(cache, TestOffset.of(5999L), batchSize);
        assertEquals(eventsAfterGap, eventsRead);

        batchSize = 150;
        eventsRead = getEvents(cache, TestOffset.of(5999L), batchSize);
        assertEquals(eventsAfterGap, eventsRead);

        batchSize = 1001;
        eventsRead = getEvents(cache, TestOffset.of(5999L), batchSize);
        assertEquals(eventsAfterGap, eventsRead);
    }

    @Test
    void removeFromEmptyCacheRemovesNothing() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        // Act
        long removed = cache.remove(TestOffset.of(1L));

        // Assert
        assertEquals(0, removed);
        assertEquals(0, cache.size());
    }

    @Test
    void removeBelowFirstEventRemovesNothing() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        cache.put(generateEvents(10L, 50)); // offsets [10..59]

        // Act
        long removed = cache.remove(TestOffset.of(10L)); // exclusive => removes < 10

        // Assert
        assertEquals(0, removed);
        assertEquals(50, cache.size());

        List<TestEvent<TestOffset>> read = cache.get(TestOffset.of(9L), 10, false);
        assertEquals(10L, read.getFirst().key().offset());
    }

    @Test
    void removeAtRangeBoundaryIsExclusiveAndDropsEarlierRanges() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        cache.put(generateEvents(1L, 2000)); // offsets [1..2000]

        // Act
        long removed = cache.remove(TestOffset.of(1000L)); // removes [1..999]

        // Assert
        assertEquals(999, removed);
        assertEquals(1001, cache.size()); // offsets [1000..2000] remain

        List<TestEvent<TestOffset>> read = cache.get(TestOffset.of(999L), 5, false);
        assertEquals(1000L, read.getFirst().key().offset());
    }

    @Test
    void removeWithinRangeRemovesOnlyBelowCutoffAndKeepsCutoffKey() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        cache.put(generateEvents(1L, 2000)); // offsets [1..2000]

        // Act
        long removed = cache.remove(TestOffset.of(1500L)); // removes [1..1499]

        // Assert
        assertEquals(1499, removed);
        assertEquals(501, cache.size()); // offsets [1500..2000]

        List<TestEvent<TestOffset>> inclusive = cache.get(TestOffset.of(1500L), 1, true);
        assertEquals(1, inclusive.size());
        assertEquals(1500L, inclusive.getFirst().key().offset());
    }

    @Test
    void removeBeyondLastRemovesAllAndIsIdempotent() {
        // Arrange
        ConcurrentEventCache<TestEvent<TestOffset>> cache
                = new ConcurrentEventCache<>(1000L, 100L);

        cache.put(generateEvents(1L, 1234)); // offsets [1..1234]

        // Act
        long removed1 = cache.remove(TestOffset.of(10_000L));
        long removed2 = cache.remove(TestOffset.of(10_000L));

        // Assert
        assertEquals(1234, removed1);
        assertEquals(0, removed2);
        assertEquals(0, cache.size());

        assertTrue(cache.get(TestOffset.of(0L), 10, false).isEmpty());
    }

    private static List<TestEvent<TestOffset>> generateEvents(long startingOffset, int count) {
        List<TestEvent<TestOffset>> events = new ArrayList<>();

        for (long i = 0; i < count; i++) {
            TestOffset offset = new TestOffset(startingOffset + i);
            TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
            events.add(event);
        }

        return events;
    }

    private static List<TestEvent<TestOffset>> getEvents(ConcurrentEventCache<TestEvent<TestOffset>> cache,
                                                       TestOffset startingOffset,
                                                       int batchSize) {
        List<TestEvent<TestOffset>> events = new ArrayList<>();
        List<TestEvent<TestOffset>> eventsRead;
        TestOffset from = startingOffset;

        do {
            eventsRead = cache.get(from, batchSize);
            if (!eventsRead.isEmpty()) {
                events.addAll(eventsRead);
                from = eventsRead.getLast().key();
            }
        } while (!eventsRead.isEmpty());

        return events;
    }
}
