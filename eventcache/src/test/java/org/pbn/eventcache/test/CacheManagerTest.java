package org.pbn.eventcache.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pbn.eventcache.CacheManager;
import org.pbn.eventcache.EventCache;
import org.pbn.eventcache.impl.ConcurrentEventCache;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheManagerTest {

    private static final long RANGE_SIZE = 100L;
    private static final long SEGMENT_SIZE = 10L;
    // Very large sweep interval so the scheduler never interferes with tests
    private static final long SWEEP_INTERVAL_MS = 60_000L;

    private CacheManager<TestEvent<?>> manager;

    @BeforeEach
    void setUp() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        manager = new CacheManager<>(scheduler, SWEEP_INTERVAL_MS);
    }

    @AfterEach
    void tearDown() {
        manager.shutdown();
    }

    // -----------------------------------------------------------------------
    // createCache(cacheName, rangeSize, segmentSize)
    // -----------------------------------------------------------------------

    @Test
    void createCache_returnsNonNullCache() {
        EventCache<TestEvent<?>> cache = manager.createCache("myCache", RANGE_SIZE, SEGMENT_SIZE);
        assertNotNull(cache);
    }

    @Test
    void createCache_sameNameReturnsSameUnderlyingCache() {
        EventCache<TestEvent<?>> first  = manager.createCache("myCache", RANGE_SIZE, SEGMENT_SIZE);
        EventCache<TestEvent<?>> second = manager.createCache("myCache", RANGE_SIZE, SEGMENT_SIZE);
        // Put into the first reference, read back via the second – they share state
        first.put(List.of(TestEvent.of(1)));
        assertEquals(1L, second.size());
    }

    @Test
    void createCache_differentNamesReturnIndependentCaches() {
        EventCache<TestEvent<?>> cacheA = manager.createCache("cacheA", RANGE_SIZE, SEGMENT_SIZE);
        EventCache<TestEvent<?>> cacheB = manager.createCache("cacheB", RANGE_SIZE, SEGMENT_SIZE);
        cacheA.put(List.of(TestEvent.of(1), TestEvent.of(2)));
        assertEquals(0L, cacheB.size());
    }

    // -----------------------------------------------------------------------
    // createCache(cacheName, rangeSize, segmentSize, ttl)
    // -----------------------------------------------------------------------

    @Test
    void createCacheWithTtl_returnsNonNullCache() {
        EventCache<TestEvent<?>> cache =
                manager.createCache("ttlCache", RANGE_SIZE, SEGMENT_SIZE, Duration.ofMinutes(5));
        assertNotNull(cache);
    }

    @Test
    void createCacheWithTtl_sameNameReturnsSameUnderlyingCache() {
        EventCache<TestEvent<?>> first =
                manager.createCache("ttlCache", RANGE_SIZE, SEGMENT_SIZE, Duration.ofMinutes(5));
        EventCache<TestEvent<?>> second =
                manager.createCache("ttlCache", RANGE_SIZE, SEGMENT_SIZE, Duration.ofMinutes(5));
        first.put(List.of(TestEvent.of(5)));
        assertEquals(1L, second.size());
    }

    // -----------------------------------------------------------------------
    // createCache(readerName, cacheName, rangeSize, segmentSize)
    // -----------------------------------------------------------------------

    @Test
    void createCacheWithReader_returnsNonNullCache() {
        EventCache<TestEvent<?>> cache =
                manager.createCache("reader1", "readerCache", RANGE_SIZE, SEGMENT_SIZE);
        assertNotNull(cache);
    }

    @Test
    void createCacheWithReader_putAndGetWork() {
        EventCache<TestEvent<?>> cache =
                manager.createCache("reader1", "readerCache", RANGE_SIZE, SEGMENT_SIZE);
        TestEvent<?> event = TestEvent.of(1);
        cache.put(List.of(event));
        assertEquals(1L, cache.size());
    }

    @Test
    void createCacheWithReader_twoReadersShareSameUnderlyingData() {
        EventCache<TestEvent<?>> reader1 =
                manager.createCache("reader1", "shared", RANGE_SIZE, SEGMENT_SIZE);
        EventCache<TestEvent<?>> reader2 =
                manager.createCache("reader2", "shared", RANGE_SIZE, SEGMENT_SIZE);

        reader1.put(List.of(TestEvent.of(1), TestEvent.of(2), TestEvent.of(3)));

        assertEquals(3L, reader2.size());
    }

    @Test
    void createCacheWithReader_sameReaderNameReturnsCacheForSameReader() {
        EventCache<TestEvent<?>> first =
                manager.createCache("reader1", "cache1", RANGE_SIZE, SEGMENT_SIZE);
        EventCache<TestEvent<?>> second =
                manager.createCache("reader1", "cache1", RANGE_SIZE, SEGMENT_SIZE);

        first.put(List.of(TestEvent.of(10)));
        // Both wrappers point to the same underlying cache
        assertEquals(1L, second.size());
    }

    // -----------------------------------------------------------------------
    // createCache(readerName, cacheName, rangeSize, segmentSize, ttl)
    // -----------------------------------------------------------------------

    @Test
    void createCacheWithReaderAndTtl_returnsNonNullCache() {
        EventCache<TestEvent<?>> cache =
                manager.createCache("reader1", "ttlReaderCache",
                        RANGE_SIZE, SEGMENT_SIZE, Duration.ofMinutes(5));
        assertNotNull(cache);
    }

    // -----------------------------------------------------------------------
    // getCache
    // -----------------------------------------------------------------------

    @Test
    void getCache_returnsUnderlyingConcurrentEventCache() {
        manager.createCache("myCache", RANGE_SIZE, SEGMENT_SIZE);
        ConcurrentEventCache<TestEvent<?>> raw = manager.getCache("myCache");
        assertNotNull(raw);
    }

    @Test
    void getCache_returnsNullForUnknownName() {
        assertNull(manager.getCache("nonExistent"));
    }

    // -----------------------------------------------------------------------
    // removeCache
    // -----------------------------------------------------------------------

    @Test
    void removeCache_returnsTrueWhenCacheExists() {
        manager.createCache("toRemove", RANGE_SIZE, SEGMENT_SIZE);
        assertTrue(manager.removeCache("toRemove"));
    }

    @Test
    void removeCache_returnsFalseWhenCacheDoesNotExist() {
        assertFalse(manager.removeCache("ghost"));
    }

    @Test
    void removeCache_cacheIsNoLongerAccessibleAfterRemoval() {
        manager.createCache("toRemove", RANGE_SIZE, SEGMENT_SIZE);
        manager.removeCache("toRemove");
        assertNull(manager.getCache("toRemove"));
    }

    @Test
    void removeCache_removingTwiceReturnsFalseSecondTime() {
        manager.createCache("toRemove", RANGE_SIZE, SEGMENT_SIZE);
        assertTrue(manager.removeCache("toRemove"));
        assertFalse(manager.removeCache("toRemove"));
    }

    // -----------------------------------------------------------------------
    // sweepCache (manual trigger)
    // -----------------------------------------------------------------------

    @Test
    void sweepCache_doesNotThrowWhenNoCachesExist() {
        assertDoesNotThrow(() -> manager.sweepCache());
    }

    @Test
    void sweepCache_doesNotThrowWithPopulatedCaches() {
        EventCache<TestEvent<?>> cache = manager.createCache("sweepMe", RANGE_SIZE, SEGMENT_SIZE);
        cache.put(List.of(TestEvent.of(1), TestEvent.of(2)));
        assertDoesNotThrow(() -> manager.sweepCache());
    }

    // -----------------------------------------------------------------------
    // shutdown
    // -----------------------------------------------------------------------

    @Test
    void shutdown_doesNotThrow() {
        assertDoesNotThrow(() -> manager.shutdown());
    }
}
