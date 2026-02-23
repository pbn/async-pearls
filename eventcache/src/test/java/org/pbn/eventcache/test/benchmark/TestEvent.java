package org.pbn.eventcache.test.benchmark;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;

import java.util.Objects;

public class TestEvent<K extends CacheKey> implements CacheValue {
    private final K cacheKey;
    private final byte[] payload;

    public TestEvent(K cacheKey, byte[] payload) {
        this.cacheKey = cacheKey;
        this.payload = payload;
    }

    public Long getOffset() {
        return cacheKey.offset();
    }

    @Override
    public K key() {
        return cacheKey;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestEvent<?> event)) return false;
        return Objects.equals(cacheKey, event.cacheKey);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cacheKey);
    }

    @Override
    public String toString() {
        return "TestEvent{" + "cacheKey=" + cacheKey + '}';
    }

    @Override
    public int eventSize() {
        return 0;
    }
}

