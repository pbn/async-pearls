package org.pbn.eventcache;

/**
 * A value that can be stored in an event cache and
 * addressed by a {@link CacheKey}.
 * <p>
 * Each cached value provides a {@linkplain #key() key}
 * which is used by the cache for ordering,
 * batching, and removal operations.
 * <p>
 * Implementations should ensure the returned key is
 * non-{@code null} and stable for the lifetime
 * of the value (i.e., it should not change while the
 * value is stored in a cache).
 *
 * @author pbn
 */
public interface CacheValue {

    /**
     * Returns the key that identifies this value in the cache.
     *
     * @param <K> the concrete key type
     * @return the key for this value
     */
    <K extends CacheKey> K key();
}
