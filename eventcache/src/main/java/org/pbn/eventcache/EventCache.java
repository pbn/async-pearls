package org.pbn.eventcache;

import java.util.List;

/**
 * A cache of offset-addressable values ("events")
 * keyed by a {@link CacheKey}.
 * <p>
 * Implementations typically store values in key order
 * (as defined by {@link CacheKey#compareTo(CacheKey)})
 * and support reading a bounded batch starting at a given key,
 * inserting values, and removing values below a cutoff key.
 *
 * @param <V> the value type stored in the cache
 *
 * @author pbn
 */
public interface EventCache<V extends CacheValue> {
    /**
     * Returns up to {@code batchSize} values starting at {@code key}.
     * <p>
     * The {@code inclusive} flag controls whether the returned batch
     * may include the value whose key equals {@code key} (if present).
     *
     * @param key the starting key for the read (must not be {@code null})
     * @param batchSize the maximum number of values to return
     * @param inclusive whether {@code key} itself is included when present
     * @param <K> the concrete key type
     * @return a list containing at most {@code batchSize} values; may be
     * empty if no values match
     * @throws IllegalArgumentException if {@code batchSize} is negative
     */
    <K extends CacheKey> List<V> get(K key, int batchSize, boolean inclusive);


    /**
     * Returns up to {@code batchSize} values starting at {@code key}.
     * <p>
     * This is a convenience overload of {@link #get(CacheKey, int, boolean)};
     * the exact inclusion behavior is implementation-defined.
     *
     * @param key the starting key for the read (must not be {@code null})
     * @param batchSize the maximum number of values to return
     * @param <K> the concrete key type
     * @return a list containing at most {@code batchSize} values; may be
     * empty if no values match
     * @throws IllegalArgumentException if {@code batchSize} is negative
     */
    <K extends CacheKey> List<V> get(K key, int batchSize);


    /**
     * Inserts the given values into the cache.
     * <p>
     * Implementations may append, overwrite, de-duplicate, and/or
     * reorder values as needed to preserve the cache's invariants.
     *
     * @param events the values to store (must not be {@code null})
     * @throws IllegalArgumentException if {@code events} contains {@code null} elements
     */
    void put(List<V> events);

    /**
     * Returns the current number of values stored in the cache.
     *
     * @return the number of cached values
     */
    long size();

    /**
     * Returns the size of all cache values in bytes.
     */
    long sizeInBytes();


    /**
     * Removes values whose key is strictly less than {@code belowThisKey}.
     *
     * @param belowThisKey cutoff key; values with keys less than this
     *                     key are removed (must not be {@code null})
     * @param <K> the concrete key type
     * @return the number of removed values
     */
    <K extends CacheKey> long remove(K belowThisKey);
}
