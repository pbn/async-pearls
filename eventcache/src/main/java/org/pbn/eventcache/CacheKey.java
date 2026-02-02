package org.pbn.eventcache;

/**
 * A comparable key for values stored in an event cache.
 * <p>
 * A {@code CacheKey} is primarily defined by its {@linkplain #offset() offset},
 * which is used for ordering and range-style operations (e.g., batch reads,
 * trimming/removal below a cutoff).
 * <p>
 * The natural ordering of {@code CacheKey} is ascending by {@code offset}.
 * This interface provides a default {@link #compareTo(CacheKey)}
 * implementation based on {@link Long#compare(long, long)}.
 *
 * <h2>Ordering contract</h2>
 * Implementations should ensure that:
 * <ul>
 *   <li>{@link #offset()} is stable and consistent for the lifetime of the key.</li>
 *   <li>{@code compareTo} is consistent with {@code equals} if keys are used in sorted sets/maps.</li>
 * </ul>
 *
 * @author pbn
 */
public interface CacheKey extends Comparable<CacheKey> {

    /**
     * Returns the numeric offset represented by this key.
     * <p>
     * Offsets typically form a monotonically increasing sequence.
     *
     * @return the offset
     */
    long offset();

    /**
     * Compares this key with the specified key for order using {@link #offset()}.
     *
     * @param o the other key to compare to
     * @return a negative integer, zero, or a positive integer as this key's offset is less than,
     * equal to, or greater than the specified key's offset
     */
    @Override
    default int compareTo(CacheKey o) {
        return Long.compare(offset(), o.offset());
    }
}
