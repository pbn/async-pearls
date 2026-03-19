package org.pbn.eventcache.test.benchmark;

import org.pbn.eventcache.CacheKey;

import java.util.Objects;

public class TestOffset implements CacheKey {
    private final long offset;

    public TestOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestOffset offset1)) return false;
        return Objects.equals(offset, offset1.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offset);
    }

    @Override
    public String toString() {
        return Objects.toString(offset);
    }

    @SuppressWarnings("unchecked")
    public static <K extends CacheKey> K of(long offset) {
        return (K)new TestOffset(offset);
    }
}
