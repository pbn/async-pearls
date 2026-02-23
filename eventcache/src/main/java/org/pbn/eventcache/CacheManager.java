package org.pbn.eventcache;

import org.jetbrains.annotations.NotNull;
import org.pbn.eventcache.impl.ConcurrentEventCache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Manages a collection of named {@link EventCache} instances and drives
 * their periodic maintenance (sweeping).
 *
 * <h2>Cache lifecycle</h2>
 * <p>Caches are created on demand via one of the {@code createCache} methods
 * and are kept alive until explicitly removed with {@link #removeCache(String)}
 * or the manager is shut down via {@link #shutdown()}.
 *
 * <h2>Reader tracking</h2>
 * <p>When a cache is created through a reader-aware method
 * (i.e. one that accepts a {@code readerName}), the manager wraps the
 * underlying cache in a thin {@code CacheReader} decorator that records the
 * offset of the last event read by that reader. This information is used
 * during sweeping to avoid evicting data that a slow reader has not yet
 * consumed.
 *
 * <h2>Sweeping</h2>
 * <p>A background sweep runs at the fixed interval supplied to the
 * constructor. Each sweep pass performs two operations on every registered
 * cache:
 * <ol>
 *   <li><b>Reader-based eviction</b> – removes all entries whose offset is
 *       below the lowest offset last read across all registered readers for
 *       that cache.</li>
 *   <li><b>TTL-based eviction</b> – removes all entries (and stale reader
 *       records) whose wall-clock insertion time is older than the cache's
 *       configured TTL.</li>
 * </ol>
 *
 * <h2>Thread safety</h2>
 * <p>All public methods are safe for concurrent use by multiple threads.
 *
 * @param <V> the type of values stored in the managed caches;
 *            must implement {@link CacheValue}
 * @author pbn
 */
public class CacheManager<V extends CacheValue> {

    // Inputs
    private final ScheduledExecutorService scheduler;

    // State
    private final ConcurrentMap<String, ConcurrentEventCache<V>> caches;
    private final CacheReaders<V> cacheReaders;

    /**
     * Creates a new {@code CacheManager}.
     *
     * <p>The supplied {@code scheduler} is used to schedule the periodic sweep
     * task immediately after construction.  The same executor is shut down
     * when {@link #shutdown()} is called.
     *
     * @param scheduler       the executor used to run the periodic sweep task;
     *                        must not be {@code null}
     * @param sweepIntervalMs the interval between consecutive sweep runs,
     *                        in milliseconds; must be positive
     */
    public CacheManager(ScheduledExecutorService scheduler, long sweepIntervalMs) {
        this.scheduler = scheduler;
        scheduler.scheduleAtFixedRate(this::sweepCache, sweepIntervalMs,
                sweepIntervalMs, TimeUnit.MILLISECONDS);
        cacheReaders = new CacheReaders<>();
        caches = new ConcurrentHashMap<>();
    }

    /**
     * Returns an {@link EventCache} identified by {@code cacheName}, creating
     * it with the given sizing parameters if it does not already exist.
     *
     * <p>The cache uses the default TTL defined by
     * {@link ConcurrentEventCache}. If a cache with the same name was
     * previously created, the existing instance is returned and the sizing
     * parameters are ignored.
     *
     * <p>The returned cache is the raw underlying cache, not wrapped in a
     * reader-tracking decorator.
     *
     * @param cacheName   a unique name for the cache; must not be {@code null}
     * @param rangeSize   the number of items per range; must be &ge; {@code segmentSize}
     * @param segmentSize the number of items per segment; must be positive
     * @return the existing or newly created {@link EventCache}; never {@code null}
     */
    public EventCache<V> createCache(String cacheName,
                                     long rangeSize,
                                     long segmentSize) {
        return caches.computeIfAbsent(cacheName, (_) ->
                new ConcurrentEventCache<>(rangeSize, segmentSize));
    }

    /**
     * Returns an {@link EventCache} identified by {@code cacheName}, creating
     * it with the given sizing parameters and TTL if it does not already exist.
     *
     * <p>If a cache with the same name was previously created, the existing
     * instance is returned and the supplied parameters are ignored.
     *
     * <p>The returned cache is the raw underlying cache, not wrapped in a
     * reader-tracking decorator.
     *
     * @param cacheName   a unique name for the cache; must not be {@code null}
     * @param rangeSize   the number of items per range; must be &ge; {@code segmentSize}
     * @param segmentSize the number of items per segment; must be positive
     * @param ttl         the time-to-live for cached entries; must not be {@code null}
     * @return the existing or newly created {@link EventCache}; never {@code null}
     */
    public EventCache<V> createCache(String cacheName,
                                     long rangeSize,
                                     long segmentSize,
                                     Duration ttl) {
        return caches.computeIfAbsent(cacheName, (_) ->
                new ConcurrentEventCache<>(rangeSize, segmentSize, ttl));
    }

    /**
     * Returns a reader-scoped view of the cache identified by
     * {@code cacheName}, creating the underlying cache if it does not already
     * exist.
     *
     * <p>The returned {@link EventCache} is a reader-tracking decorator: every
     * call to {@code get} updates the last-read offset for {@code readerName},
     * which the sweep pass uses to avoid evicting data not yet consumed by
     * slow readers.
     *
     * <p>Calling this method more than once with the same
     * {@code (readerName, cacheName)} pair returns a new decorator backed by
     * the same reader state and the same underlying cache, so the offset
     * tracking remains consistent.
     *
     * <p>The underlying cache uses the default TTL defined by
     * {@link ConcurrentEventCache}.
     *
     * @param readerName  a unique name identifying the reader; must not be {@code null}
     * @param cacheName   a unique name for the cache; must not be {@code null}
     * @param rangeSize   the number of items per range; must be &ge; {@code segmentSize}
     * @param segmentSize the number of items per segment; must be positive
     * @return a reader-tracking {@link EventCache} view; never {@code null}
     */
    public EventCache<V> createCache(String readerName,
                                     String cacheName,
                                     long rangeSize,
                                     long segmentSize) {
        return createCache(readerName, cacheName, () ->
                new ConcurrentEventCache<>(rangeSize, segmentSize));
    }

    /**
     * Returns a reader-scoped view of the cache identified by
     * {@code cacheName}, creating the underlying cache with the given TTL if
     * it does not already exist.
     *
     * <p>Behaves identically to
     * {@link #createCache(String, String, long, long)} except that the cache
     * is created with the specified {@code ttl} when it does not already exist.
     *
     * @param readerName  a unique name identifying the reader; must not be {@code null}
     * @param cacheName   a unique name for the cache; must not be {@code null}
     * @param rangeSize   the number of items per range; must be &ge; {@code segmentSize}
     * @param segmentSize the number of items per segment; must be positive
     * @param ttl         the time-to-live for cached entries; must not be {@code null}
     * @return a reader-tracking {@link EventCache} view; never {@code null}
     */
    public EventCache<V> createCache(String readerName,
                                     String cacheName,
                                     long rangeSize,
                                     long segmentSize,
                                     Duration ttl) {
        return createCache(readerName, cacheName, () ->
                new ConcurrentEventCache<>(rangeSize, segmentSize, ttl));
    }

    private EventCache<V> createCache(String readerName,
                                      String cacheName,
                                      Supplier<ConcurrentEventCache<V>> cacheFactory) {
        AtomicReference<EventCache<V>> cacheRef = new AtomicReference<>();
        caches.compute(cacheName, (_, v) -> {
            ConcurrentEventCache<V> cache = v;
            if (cache == null) {
                cache = cacheFactory.get();
            }

            cacheRef.set(cacheReaders.createCacheReader(readerName, cacheName, cache));
            return cache;
        });

        return cacheRef.get();
    }

    /**
     * Removes the cache identified by {@code cacheName} and all reader
     * tracking information associated with it.
     *
     * <p>Any {@link EventCache} references previously returned for this cache
     * name remain usable as Java objects but are no longer managed by this
     * {@code CacheManager}, meaning they will not be swept.
     *
     * @param cacheName the name of the cache to remove; must not be {@code null}
     * @return {@code true} if the cache existed and was removed;
     *         {@code false} if no cache with that name was registered
     */
    public boolean removeCache(String cacheName) {
        AtomicBoolean result = new AtomicBoolean(false);

        caches.computeIfPresent(cacheName, (k, v) -> {
            result.set(true);
            cacheReaders.removeReaderInfos(cacheName);
            return null;
        });

        return result.get();
    }

    /**
     * Shuts down the background sweep scheduler.
     *
     * <p>After this call, the scheduler will no longer accept new tasks and no
     * further sweeps will be triggered. In-progress sweeps are allowed to
     * complete.
     *
     * @throws RuntimeException if the scheduler throws an unexpected exception
     *                          during shutdown
     */
    public void shutdown() {
        try {
            scheduler.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the raw {@link ConcurrentEventCache} registered under
     * {@code cacheName}, or {@code null} if no such cache exists.
     *
     * @param cacheName the name of the cache to look up; must not be {@code null}
     * @return the underlying cache instance, or {@code null} if not found
     */
    public ConcurrentEventCache<V> getCache(String cacheName) {
        return caches.get(cacheName);
    }

    /**
     * Performs a single sweep pass over all registered caches.
     *
     * <p>For each cache the sweep:
     * <ol>
     *   <li>Removes entries below the lowest offset last read by any
     *       registered reader (reader-based eviction).</li>
     *   <li>Removes entries older than the cache's TTL and purges stale
     *       reader records for that cache (TTL-based eviction).</li>
     * </ol>
     *
     * <p>This method is normally invoked automatically by the background
     * scheduler, but may also be called directly (e.g. in tests or for
     * on-demand maintenance). Any {@link Throwable} thrown during the sweep
     * is silently suppressed to ensure the scheduler thread stays alive.
     */
    public void sweepCache() {
        try {
            caches.forEach((cacheName, cache) -> {
                sweepByLowestReader(cacheName, cache);
                sweepIdleCache(cacheName, cache);
            });
        } catch (Throwable e) {
            // Log here
        }
    }

    private ConcurrentMap<String, ReaderInfo> getReaderInfos(String cacheName) {
        return cacheReaders.getReaderInfos(cacheName);
    }

    private void sweepIdleCache(String cacheName,  ConcurrentEventCache<V> cache) {
        Duration ttl = cache.getTtl();
        long cutOffTime = System.currentTimeMillis() - ttl.toMillis();
        cache.remove(cutOffTime);
        cacheReaders.removeReaderInfos(cacheName, cutOffTime);
    }

    private void sweepByLowestReader(String cacheName, ConcurrentEventCache<V> cache) {
        Optional<ReaderInfo> lowestReader = getLowestOffsetReader(cacheName);
        lowestReader.ifPresent(readerInfo -> cache.remove(readerInfo.getLastReadOffset()));
    }

    private Optional<ReaderInfo> getLowestOffsetReader(String cacheName) {
        ConcurrentMap<String, ReaderInfo> readerInfos = getReaderInfos(cacheName);
        return readerInfos.values().stream().sorted().findFirst();
    }

    private static class CacheReaders<V extends CacheValue> {

        // {cacheName -> {readerName -> ReaderInfo}}
        private final ConcurrentMap<String, ConcurrentMap<String, ReaderInfo>> cacheNameToReaderInfos =
                new ConcurrentHashMap<>();

        public CacheReaders() {
        }

        EventCache<V> createCacheReader(String readerName,
                                        String cacheName,
                                        EventCache<V> cache) {
            ConcurrentMap<String, ReaderInfo> readerInfos =
                    this.cacheNameToReaderInfos.computeIfAbsent(cacheName,
                            (_) -> new ConcurrentHashMap<>());


            ReaderInfo readerInfo = readerInfos.computeIfAbsent(readerName,
                    (String _) -> new ReaderInfo(readerName));

            return new CacheReader<>(readerInfo, cache);
        }

        public ConcurrentMap<String, ReaderInfo> getReaderInfos(String cacheName) {
            return this.cacheNameToReaderInfos.get(cacheName);
        }

        public void removeReaderInfos(String cacheName) {
            this.cacheNameToReaderInfos.remove(cacheName);
        }

        public void removeReaderInfos(String cacheName,
                                      long olderThanThisTimestamp) {
            List<String> readersToRemove = new ArrayList<>();
            cacheNameToReaderInfos.computeIfPresent(cacheName, (_, v) -> {
                v.forEach((key, value) -> {
                    if (value.getLastReadTimestamp() < olderThanThisTimestamp) {
                        readersToRemove.add(key);
                    }
                });
                return v;
            });

            ConcurrentMap<String, ReaderInfo> readerInfos = this.cacheNameToReaderInfos.get(cacheName);
            for (String readerName : readersToRemove) {
                readerInfos.computeIfPresent(readerName, (_, v) -> {
                    if (v.getLastReadTimestamp() < olderThanThisTimestamp) {
                        return null;
                    }
                    return v;
                });
            }
        }

        private record CacheReader<V extends CacheValue>(ReaderInfo readerInfo,
                                                         EventCache<V> cache)
                implements EventCache<V> {

            @Override
            public <K extends CacheKey> List<V> get(K key,
                                                    int batchSize,
                                                    boolean inclusive) {
                List<V> results = cache.get(key, batchSize, inclusive);
                recordLastOffset(results);
                return results;
            }

            @Override
            public <K extends CacheKey> List<V> get(K key, int batchSize) {
                List<V> results = cache.get(key, batchSize);
                recordLastOffset(results);
                return results;
            }

            private void recordLastOffset(List<V> results) {
                V last = results.getLast();
                if (last != null) {
                    readerInfo.touch(last.key().offset());
                }
            }

            @Override
            public void put(List<V> events) {
                cache.put(events);
            }

            @Override
            public long size() {
                return cache.size();
            }

            @Override
            public long sizeInBytes() {
                return cache.sizeInBytes();
            }

            @Override
            public <K extends CacheKey> long remove(K belowThisKey) {
                return cache.remove(belowThisKey);
            }

            @Override
            public long remove(long olderThanThisTimestamp) {
                return cache.remove(olderThanThisTimestamp);
            }
        }
    }

    private static class ReaderInfo implements Comparable<ReaderInfo> {
        private final String readerName;

        private final AtomicLong lastReadOffset = new AtomicLong(-1);
        private final AtomicLong lastReadTimestamp = new AtomicLong(-1);

        public ReaderInfo(String readerName) {
            this.readerName = readerName;
        }

        public String getReaderName() {
            return readerName;
        }

        public long getLastReadOffset() {
            return lastReadOffset.get();
        }

        public long getLastReadTimestamp() {
            return lastReadTimestamp.get();
        }

        public void touch(long offset) {
            lastReadOffset.set(offset);
            lastReadTimestamp.set(System.currentTimeMillis());
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ReaderInfo that = (ReaderInfo) o;
            return Objects.equals(readerName, that.readerName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(readerName);
        }

        @Override
        public int compareTo(@NotNull ReaderInfo o) {
            return Long.compare(lastReadOffset.get(), o.lastReadOffset.get());
        }
    }

}
