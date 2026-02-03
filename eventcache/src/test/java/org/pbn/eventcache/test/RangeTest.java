


package org.pbn.eventcache.test;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;
import org.pbn.eventcache.Range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Range class
 */
public class RangeTest {

    private Range<TestEvent> range;
    private static final long RANGE_INDEX = 1L;
    private static final long SEGMENT_SIZE = 10L;

    // Test implementation of CacheKey
    record TestOffset(long offset) implements CacheKey {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestOffset(long offset1))) {
                return false;
            }
            return offset == offset1;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(offset);
        }

        @NotNull
        @Override
        public String toString() {
            return "TestOffset{offset=" + offset + '}';
        }
    }

    // Test implementation of CacheValue
    record TestEvent(TestOffset key, String data) implements CacheValue {
        public TestEvent(long offset, String data) {
            this(new TestOffset(offset), data);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestEvent(TestOffset key1, String data1))) return false;
            return key.equals(key1) && data.equals(data1);
        }

        @Override
        public int eventSize() {
            return data.getBytes().length;
        }

        @NotNull
        @Override
        public String toString() {
            return "TestEvent{key=" + key + ", data='" + data + "'}";
        }
    }

    @BeforeEach
    void setUp() {
        range = new Range<>(RANGE_INDEX, SEGMENT_SIZE);
    }

    @Test
    void testConstructor() {
        assertEquals(RANGE_INDEX, range.getRangeIndex());
        assertEquals(0, range.size());
    }

    @Test
    void testPutSingleItem() {
        TestOffset key = new TestOffset(5L);
        TestEvent value = new TestEvent(key, "test-data");

        range.put(key, value);

        assertEquals(1, range.size());
    }

    @Test
    void testPutMultipleItemsSameSegment() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(5L, "data2");
        TestEvent event3 = new TestEvent(9L, "data3");

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);
        range.put(event3.key(), event3);

        assertEquals(3, range.size());
        assertEquals(1, range.segmentsSize());

    }

    @Test
    void testPutMultipleItemsDifferentSegments() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(15L, "data2");  // Different segment
        TestEvent event3 = new TestEvent(25L, "data3");  // Another segment

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);
        range.put(event3.key(), event3);

        assertEquals(3, range.size());
        assertEquals(3, range.segmentsSize());
    }

    @Test
    void testPutList() {
        List<TestEvent> events = Arrays.asList(
                new TestEvent(1L, "data1"),
                new TestEvent(5L, "data2"),
                new TestEvent(15L, "data3")
        );

        boolean result = range.put(events);

        assertTrue(result);
        assertEquals(3, range.size());
        assertEquals(2, range.segmentsSize());
    }

    @Test
    void testPutEmptyList() {
        List<TestEvent> emptyList = List.of();
        boolean result = range.put(emptyList);

        assertTrue(result);
        assertEquals(0, range.size());
        assertEquals(0, range.segmentsSize());
    }

    @Test
    void testGetInclusive() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(5L, "data2");
        TestEvent event3 = new TestEvent(9L, "data3");

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);
        range.put(event3.key(), event3);

        List<TestEvent> result = range.get(new TestOffset(1L), 10, true);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    void testGetExclusive() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(5L, "data2");
        TestEvent event3 = new TestEvent(15L, "data3"); // Different segment

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);
        range.put(event3.key(), event3);

        List<TestEvent> result = range.get(new TestOffset(9L), 10, false);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetFromEmptyRange() {
        List<TestEvent> result = range.get(new TestOffset(1L), 10, true);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetFromNonExistentSegment() {
        TestEvent event = new TestEvent(1L, "data1");
        range.put(event.key(), event);

        // Try to get from a segment that doesn't exist
        List<TestEvent> result = range.get(new TestOffset(100L), 10, true);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testRemove() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(5L, "data2");
        TestEvent event3 = new TestEvent(15L, "data3");
        TestEvent event4 = new TestEvent(25L, "data4");

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);
        range.put(event3.key(), event3);
        range.put(event4.key(), event4);

        assertEquals(4, range.size());

        // Remove items below offset 16 (exclusive)
        long removed = range.remove(new TestOffset(16L));

        assertTrue(removed > 0);
        assertTrue(range.size() < 4);
    }

    @Test
    void testRemoveFromEmptyRange() {
        long removed = range.remove(new TestOffset(10L));
        assertEquals(0, removed);
        assertEquals(0, range.size());
    }

    @Test
    void testRemoveAllItems() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(5L, "data2");

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);

        assertEquals(2, range.size());

        // Remove all items
        long removed = range.remove(new TestOffset(100L));

        assertEquals(2, removed);
        assertEquals(0, range.size());
    }

    @Test
    void testSize() {
        assertEquals(0, range.size());

        TestEvent event1 = new TestEvent(1L, "data1");
        range.put(event1.key(), event1);
        assertEquals(1, range.size());

        TestEvent event2 = new TestEvent(15L, "data2");
        range.put(event2.key(), event2);
        assertEquals(2, range.size());
    }

    @Test
    void testIsLast() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(5L, "data2");

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);

        // Test with the last key in the range
        boolean isLast = range.isLast(new TestOffset(5L));
        assertTrue(isLast);

        // Test with a key that's not the last
        boolean isNotLast = range.isLast(new TestOffset(1L));
        assertFalse(isNotLast);
    }

    @Test
    void testIsLastEmptyRange() {
        // Should handle empty range gracefully
        assertThrows(Exception.class, () -> range.isLast(new TestOffset(1L)));
    }

    @Test
    void testToString() {
        String result = range.toString();

        assertNotNull(result);
        assertTrue(result.contains("Range{"));
        assertTrue(result.contains("rangeIndex=" + RANGE_INDEX));
        assertTrue(result.contains("segmentSize=" + SEGMENT_SIZE));
        assertTrue(result.contains("segmentsCount=0"));
    }

    @Test
    void testToStringWithData() {
        TestEvent event = new TestEvent(1L, "data1");
        range.put(event.key(), event);

        String result = range.toString();

        assertNotNull(result);
        assertTrue(result.contains("segmentsCount=1"));
    }

    @Test
    void testDeepToString() {
        String result = range.deepToString();

        assertNotNull(result);
        assertTrue(result.contains("r[" + RANGE_INDEX + "]"));
        assertTrue(result.contains("nos=0"));
    }

    @Test
    void testDeepToStringWithData() {
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(15L, "data2");

        range.put(event1.key(), event1);
        range.put(event2.key(), event2);

        String result = range.deepToString();

        assertNotNull(result);
        assertTrue(result.contains("nos=2")); // Number of segments
    }

    @Test
    void testConcurrentOperations() {
        // Test basic thread safety by performing concurrent puts
        TestEvent event1 = new TestEvent(1L, "data1");
        TestEvent event2 = new TestEvent(2L, "data2");
        TestEvent event3 = new TestEvent(3L, "data3");

        // These should work without throwing exceptions
        range.put(event1.key(), event1);
        range.put(event2.key(), event2);
        range.put(event3.key(), event3);

        assertEquals(3, range.size());
    }

    @Test
    void testSegmentBoundaries() {
        // Test items that fall on segment boundaries
        // With segment size 10, offsets 10, 20, 30 should be in different segments
        TestEvent event10 = new TestEvent(10L, "data10");
        TestEvent event20 = new TestEvent(20L, "data20");
        TestEvent event30 = new TestEvent(30L, "data30");

        range.put(event10.key(), event10);
        range.put(event20.key(), event20);
        range.put(event30.key(), event30);

        assertEquals(3, range.size());
        assertEquals(3, range.segmentsSize());
    }

    @Test
    void testRangeIndex() {
        assertEquals(RANGE_INDEX, range.getRangeIndex());

        // Test with different range index
        Range<TestEvent> range2 = new Range<>(99L, SEGMENT_SIZE);
        assertEquals(99L, range2.getRangeIndex());
    }


    @Test
    void removeIsExclusive_andCountsRemoved_acrossSegments() {
        // segmentSize=10 => segment 0: [0..9], segment 1: [10..19], segment 2: [20..29]
        Range<TestEvent> range = new Range<>(1L, 10L);

        putRange(range, 1, 30); // offsets 1..30 inclusive
        long removed = range.remove(new TestOffset(16L)); // exclusive: removes 1..15
        assertEquals(15L, removed);
        assertEquals(15L, range.size()); // 16..30 remain

        // sanity: we can still read from just before 16 (exclusive get)
        List<TestEvent> got = range.get(new TestOffset(15L), 100, false);
        assertFalse(got.isEmpty());
        assertEquals(16L, got.getFirst().key().offset());
        assertEquals(30L, got.getLast().key().offset());
    }

    @Test
    void removeBelowFirstKey_removesNothing() {
        Range<TestEvent> range = new Range<>(0L, 10L);
        putRange(range, 10, 5); // 10..14

        long removed = range.remove(new TestOffset(10L)); // exclusive => nothing < 10 inside this range
        assertEquals(0L, removed);
        assertEquals(5L, range.size());

        List<TestEvent> got = range.get(new TestOffset(9L), 10, false);
        assertEquals(5, got.size());
        assertEquals(10L, got.getFirst().key().offset());
    }

    @Test
    void removeAboveLastKey_removesAllItems_andMayDropSegments() {
        Range<TestEvent> range = new Range<>(0L, 10L);
        putRange(range, 1, 25); // 1..25

        long removed = range.remove(new TestOffset(26L)); // exclusive => removes 1..25
        assertEquals(25L, removed);
        assertEquals(0L, range.size());
        assertEquals(0, range.segmentsSize());
    }

    @Test
    void removeFromEmptyRange_isNoOp() {
        Range<TestEvent> range = new Range<>(0L, 10L);

        long removed = range.remove(new TestOffset(123L));
        assertEquals(0L, removed);
        assertEquals(0L, range.size());
        assertEquals(0, range.segmentsSize());
    }

    private static void putRange(Range<TestEvent> range, long startOffset, int count) {
        List<TestEvent> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            items.add(new TestEvent(startOffset + i, String.valueOf(i)));
        }
        range.put(items);
    }
}
