package org.pbn.eventcache;

/**
 * An object that is indexable using offset.
 *
 * @author pbn
 */
public interface Indexable {
    /**
     * Returns a location (index) of a bucket given the
     * offset aware cache key and the bucket size.
     *
     * @param offset an offset
     * @param bucketSize size of a bucket
     * @return index of the bucket that contains the given
     * cache key.
     * @throws IllegalArgumentException if the given offset
     * is a negative number.
     */
    default long getIndex(Long offset, Long bucketSize) {
        if (offset < 0) {
            throw new IllegalArgumentException("Negative offset number is not accepted");
        }

        return (long)Math.floor((double) (offset - 1) / bucketSize) + 1;
    }
}
