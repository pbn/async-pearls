package org.pbn.eventcache.test.benchmark;

import org.pbn.eventcache.CacheKey;
import org.pbn.eventcache.CacheValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class EventSource<V extends CacheValue> {

    private final Function<Long, V> eventCreator;

    public EventSource(Function<Long, V> eventCreator) {
        this.eventCreator = eventCreator;
    }

    public <K extends CacheKey> List<V> get(K from, int batchSize, boolean inclusive) {
        List<V> events = new ArrayList<>();
        int index = 0;
        long start = inclusive ? from.offset() : from.offset() + 1L;
        for (; index < batchSize; index++) {
            events.add(eventCreator.apply(start));
            start++;
        }

        return events;
    }
}
