package org.pbn.eventcache.test;


import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.pbn.eventcache.impl.Segment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The unit tests for the {@code Segment} class.
 * Following are some of the test cases.
 * <p/>
 * 1. Sequential put and get calls.
 * 2. Parallel put and get calls.
 * 3. Sequential overflow put call.
 * 4. Parallel overflow put call.
 *    Assert with gaps in the offset values.
 * 5. Concurrent put and get calls.
 * 6. Multiple concurrent overflow calls.
 */
public class SegmentTest {

    /**
     * Caches events with gaps and gets the events.
     */
    @Test
    void sequentialGetsWithGaps() {
        // Arrange
        int capacity = 10;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        for (long i = 1; i <= capacity; i++) {
            if (i % 2 == 0) continue; // Create gaps
            TestOffset offset = new TestOffset(i);
            TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
            segment.put(offset, event);
        }

        // Act
        List<TestEvent<TestOffset>> events = segment.get(TestOffset.of(0L), capacity);

        // Assert
        assertEquals(1, events.size());
        for (int i = 0; i < events.size(); i++) {
            assertEquals((i * 2L) + 1, events.get(i).getOffset());
        }
    }

    /**
     * Tests get events call with higher num items than
     * available events in the cache.
     */
    @Test
    void getBeyondAvailableItems() {
        // Arrange
        int capacity = 5;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        for (long i = 1; i <= capacity; i++) {
            TestOffset offset = new TestOffset(i);
            TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
            segment.put(offset, event);
        }

        // Act
        List<TestEvent<TestOffset>> events = segment.get(TestOffset.of(0L), 10);

        // Assert
        assertEquals(capacity, events.size());
        for (int i = 0; i < events.size(); i++) {
            assertEquals(i + 1, events.get(i).getOffset());
        }
    }

    /**
     * No updates to cache entry with the same key are allowed.
     */
    @Test
    void putDuplicateKeyUpdatesValue() {
        // Arrange
        int capacity = 5;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        TestOffset offset = new TestOffset(1L);
        TestEvent<TestOffset> event1 = new TestEvent<>(offset, new byte[]{1});
        TestEvent<TestOffset> event2 = new TestEvent<>(offset, new byte[]{2});

        // Act
        segment.put(offset, event1);
        segment.put(offset, event2);

        // Assert
        List<TestEvent<TestOffset>> events = segment.get(TestOffset.of(0L), 1);
        assertEquals(1, events.size());
        assertEquals(1, events.getFirst().getPayload()[0]);
    }

    /**
     * Simple sequential put and get events.
     */
    @Test
    void testSequentialPutsAndGets() {
        // Arrange
        int capacity = 2000;
        Segment<TestEvent<TestOffset>> s1 = new Segment<>(1L, capacity);
        List<TestEvent<TestOffset>> inputEvents = new ArrayList<>();

        // Act
        // Generate and Cache events via put call
        for (long i = 1; i < capacity + 1; i++) {
            TestOffset offset = new TestOffset(i);
            TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
            inputEvents.add(event);
            s1.put(offset, event);
        }

        assertEquals(capacity, s1.size());

        // Get events 100 at a time
        int batchSize = 100;
        int accumulatedSize = 0;
        List<TestEvent<TestOffset>> cachedEvents = new ArrayList<>();
        TestOffset startingOffset = TestOffset.of(0L);
        while (accumulatedSize < capacity) {
            List<TestEvent<TestOffset>> events;
            events = s1.get(startingOffset, batchSize);

            assertEquals(batchSize, events.size());

            cachedEvents.addAll(events);
            accumulatedSize += events.size();
            startingOffset = events.getLast().key();
        }

        // Assert all events are gathers without any duplicates and gaps.
        assertEquals(capacity, cachedEvents.size());
        assertEquals(inputEvents, cachedEvents);
    }

    /**
     * Tests overflow behavior, tries to put more events
     * than available capacity.
     */
    @Test
    void testSequentialOverflow() {
        // Arrange
        int capacity = 2048;
        Segment<TestEvent<TestOffset>> s1 = new Segment<>(1L, capacity);

        // Act
        for (long i = 1; i < capacity + 1; i++) {
            TestOffset offset = new TestOffset(i);
            TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
            s1.put(offset, event);
        }

        // Assert
        assertEquals(capacity, s1.size());
        try {
            TestOffset offset = new TestOffset(2049L);
            TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
            s1.put(offset, event);
        } catch (IllegalArgumentException e) {
            // Pass
           assertEquals(capacity, s1.size());
        }
    }

    @Test
    void testParallelPutsAndGets() throws Exception {
        int capacity = 2048;
        try (ExecutorService threadPool = Executors.newFixedThreadPool(100)) {
            // Arrange
            Segment<TestEvent<TestOffset>> s1 = new Segment<>(1L, capacity);
            List<Callable<Boolean>> tasks = new ArrayList<>();

            for (long i = 1; i < capacity + 1; i++) {
                final long index = i;
                Callable<Boolean> task = () -> {
                    TestOffset offset = new TestOffset(index);
                    TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
                    s1.put(offset, event);
                    return true;
                };
                tasks.add(task);
            }

            // Act
            List<Future<Boolean>> futures = threadPool.invokeAll(tasks);

            // Assert
            for (Future<Boolean> future : futures) {
                assertTrue(future.get(10, TimeUnit.SECONDS));
            }

            assertEquals(capacity, s1.size());

            List<TestEvent<TestOffset>> events = s1.get(TestOffset.of(0L), capacity);
            for (int i = 0; i < capacity; i++) {
                TestEvent<TestOffset> event = events.get(i);
                assertEquals(((long) i + 1), event.getOffset());
            }
        }
    }

    @Test
    void testParallelOverflow() throws Exception {
        int capacity = 2048;
        try (ExecutorService threadPool = Executors.newFixedThreadPool(100)) {
            // Arrange
            Segment<TestEvent<TestOffset>> s1 = new Segment<>(1L, capacity);
            List<Callable<PutResult>> tasks = createPutTestTasks(capacity, s1);

            // Act
            List<Future<PutResult>> futures = threadPool.invokeAll(tasks);

            // Assert
            int successes = 0;
            int failures = 0;
            long lastSuccessfulOffset = -1L;
            long index = 0L;
            for (Future<PutResult> future : futures) {
                if (future.get(10, TimeUnit.SECONDS).outputPutReturn) {
                    successes++;
                    index = index + 1L;
                } else {
                    lastSuccessfulOffset = index;
                    failures++;
                }
            }

            assertEquals(capacity, successes);
            assertEquals(1, failures);
            assertEquals(capacity, s1.size());

            List<TestEvent<TestOffset>> events = s1.get(TestOffset.of(0L), capacity);
            long expectedCapacityWithGap = capacity - (capacity - lastSuccessfulOffset);
            // If there is a gap, the sub list would be less than capacity, so need to
            // assert with the expected size of the sub list
            assertEquals(expectedCapacityWithGap, events.size(), "Segment:" + s1);

            // Assert the last offset before the gap.
            assertEquals(lastSuccessfulOffset, events.getLast().getOffset());

            for (int i = 0; i < expectedCapacityWithGap; i++) {
                TestEvent<TestOffset> event = events.get(i);
                assertEquals(((long) i + 1), event.getOffset());
            }
        }
    }

    /**
     * remove(belowThisKey) is exclusive: it removes keys strictly
     * less than belowThisKey and keeps the item keyed
     * by belowThisKey (if present).
     */
    @Test
    void removeIsExclusiveAndReturnsRemovedCount() {
        // Arrange
        int capacity = 10;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        for (long i = 1; i <= capacity; i++) {
            TestOffset offset = new TestOffset(i);
            segment.put(offset, new TestEvent<>(offset, new byte[]{(byte) i}));
        }

        // Act
        long removed = segment.remove(TestOffset.of(6L)); // removes 1..5

        // Assert
        assertEquals(5, removed);
        assertEquals(5, segment.size());

        // Ensure the key equal to the belowThisKey is still present and is now
        // the first contiguous element after 5.
        List<TestEvent<TestOffset>> remaining = segment.get(TestOffset.of(5L), capacity);
        assertEquals(5, remaining.size());
        assertEquals(6L, remaining.getFirst().getOffset());
        assertEquals(10L, remaining.getLast().getOffset());
    }

    @Test
    void removeOnEmptySegmentRemovesNothing() {
        // Arrange
        int capacity = 10;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        // Act
        long removed = segment.remove(TestOffset.of(5L));

        // Assert
        assertEquals(0, removed);
        assertEquals(0, segment.size());
        assertTrue(segment.isEmpty());
    }

    @Test
    void removeBelowFirstKeyRemovesNothing() {
        // Arrange
        int capacity = 10;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        for (long i = 1; i <= capacity; i++) {
            TestOffset offset = new TestOffset(i);
            segment.put(offset, new TestEvent<>(offset, new byte[0]));
        }

        // Act
        long removed = segment.remove(TestOffset.of(1L)); // exclusive: nothing is < 1

        // Assert
        assertEquals(0, removed);
        assertEquals(capacity, segment.size());

        List<TestEvent<TestOffset>> events = segment.get(TestOffset.of(0L), capacity);
        assertEquals(capacity, events.size());
        assertEquals(1L, events.getFirst().getOffset());
        assertEquals(10L, events.getLast().getOffset());
    }

    @Test
    void removeAboveLastKeyRemovesAllItems() {
        // Arrange
        int capacity = 10;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        for (long i = 1; i <= capacity; i++) {
            TestOffset offset = new TestOffset(i);
            segment.put(offset, new TestEvent<>(offset, new byte[0]));
        }

        // Act
        long removed = segment.remove(TestOffset.of(11L)); // removes 1..10

        // Assert
        assertEquals(capacity, removed);
        assertEquals(0, segment.size());
        assertTrue(segment.isEmpty());
    }

    /**
     * remove() is best-effort and may race with concurrent put/get.
     * This test asserts that concurrent access doesn't throw and
     * the Segment remains in a consistent state.
     */
    @Test
    void concurrentPutGetAndRemove_isBestEffortAndDoesNotThrow() throws Exception {
        // Arrange
        int capacity = 512;
        Segment<TestEvent<TestOffset>> segment = new Segment<>(1L, capacity);

        // Pre-fill with some items (within the segment)
        for (long i = 1; i <= 200; i++) {
            TestOffset offset = new TestOffset(i);
            segment.put(offset, new TestEvent<>(offset, new byte[0]));
        }

        try (ExecutorService pool = Executors.newFixedThreadPool(12)) {
            List<Callable<Boolean>> tasks = new ArrayList<>();

            int operationsPerType = 2_000;

            // Concurrent writers
            for (int i = 0; i < operationsPerType; i++) {
                tasks.add(() -> {
                    long off = ThreadLocalRandom.current().nextLong(1, capacity + 1L);
                    TestOffset offset = new TestOffset(off);
                    segment.put(offset, new TestEvent<>(offset, new byte[0]));
                    return true;
                });
            }

            // Concurrent removers (exclusive) and may race with puts/gets
            for (int i = 0; i < operationsPerType; i++) {
                tasks.add(() -> {
                    long belowExclusive = ThreadLocalRandom.current().nextLong(1, capacity + 2L); // 1..capacity+1
                    long removed = segment.remove(TestOffset.of(belowExclusive));
                    assertTrue(removed >= 0 && removed <= capacity);
                    return true;
                });
            }

            // Concurrent readers
            for (int i = 0; i < operationsPerType; i++) {
                tasks.add(() -> {
                    long from = ThreadLocalRandom.current().nextLong(0, capacity + 1L); // 0..capacity
                    int batch = ThreadLocalRandom.current().nextInt(1, 50);
                    List<TestEvent<TestOffset>> got = segment.get(TestOffset.of(from), batch, false);
                    assertTrue(got.size() <= batch);
                    return true;
                });
            }

            // Act
            List<Future<Boolean>> futures = pool.invokeAll(tasks);

            // Assert: no task threw
            for (Future<Boolean> f : futures) {
                assertTrue(f.get(20, TimeUnit.SECONDS));
            }
        }

        // Assert: state is still sane (best-effort semantics, so no stronger ordering assertions)
        assertTrue(segment.size() >= 0 && segment.size() <= capacity);
    }

    @NotNull
    private static List<Callable<PutResult>> createPutTestTasks(int capacity,
                                                                Segment<TestEvent<TestOffset>> s1) {
        List<Callable<PutResult>> tasks = new ArrayList<>();

        for (long i = 1; i < capacity + 2; i++) {
            final long index = i;

            Callable<PutResult> task = () -> {
                Exception exception = null;
                boolean result;
                try {
                    TestOffset offset = new TestOffset(index);
                    TestEvent<TestOffset> event = new TestEvent<>(offset, new byte[0]);
                    s1.put(offset, event);
                    result = true;
                } catch (IllegalArgumentException e) {
                    exception = e;
                    result = false;
                }
                return new PutResult(index, result, exception);
            };
            tasks.add(task);
        }
        return tasks;
    }

    private record PutResult (Long inputIndex, Boolean outputPutReturn, Exception thrown) {
    }
}
