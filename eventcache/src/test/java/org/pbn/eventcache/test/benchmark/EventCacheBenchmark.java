package org.pbn.eventcache.test.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.pbn.eventcache.EventCache;
import org.pbn.eventcache.impl.ConcurrentEventCache;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class EventCacheBenchmark {

    @Param({"1000000", "5000000", "10000000", "15000000"})
    int eventsCount;

    @Param({"2000"})
    int segmentSize;

    @Param({"100000"})
    int rangeSize;

    @Benchmark
    public void baseLine() throws Exception {
    }

    @Benchmark
    public List<TestEvent<TestOffset>> testEventReaderWithSimpleEventCache() throws Exception {
        EventSource<TestEvent<TestOffset>> eventSource
                = new EventSource<>((offset) -> new TestEvent<>(TestOffset.of(offset), new byte[0]));

        EventCache<TestEvent<TestOffset>> simpleCache = new SimpleEventCache<>();

        return readEvents(eventSource, simpleCache);
    }

    @Benchmark
    public List<TestEvent<TestOffset>> testEventReaderWithConcurrentEventCache() throws Exception {
        EventSource<TestEvent<TestOffset>> eventSource
                = new EventSource<>((offset) -> new TestEvent<>(TestOffset.of(offset), new byte[0]));

        EventCache<TestEvent<TestOffset>> ceCache = new ConcurrentEventCache<>(rangeSize, segmentSize);

        return readEvents(eventSource, ceCache);
    }

    private List<TestEvent<TestOffset>> readEvents(EventSource<TestEvent<TestOffset>> eventSource,
                                                   EventCache<TestEvent<TestOffset>> ceCache)
            throws InterruptedException {
        long endOffset = eventsCount - 1; // The starting offset is 0
        List<Thread> threads = new LinkedList<>();
        List<EventReaderTask<TestOffset, TestEvent<TestOffset>>> readerTasks = new LinkedList<>();

        EventReaderTask<TestOffset, TestEvent<TestOffset>> reader1
                = new EventReaderTask<>(0L, endOffset, segmentSize, eventsCount, eventSource, ceCache);

        readerTasks.add(reader1);
        threads.add(new Thread(reader1));

        EventReaderTask<TestOffset, TestEvent<TestOffset>> reader2
                = new EventReaderTask<>(0L, endOffset, segmentSize, eventsCount, eventSource, ceCache);

        readerTasks.add(reader2);
        threads.add(new Thread(reader2));

        EventReaderTask<TestOffset, TestEvent<TestOffset>> reader3
                = new EventReaderTask<>(0L, endOffset, segmentSize, eventsCount, eventSource, ceCache);

        readerTasks.add(reader3);
        threads.add(new Thread(reader3));

        EventReaderTask<TestOffset, TestEvent<TestOffset>> midReader
                = new EventReaderTask<>(eventsCount/2, endOffset,
                segmentSize, eventsCount/2,
                eventSource, ceCache);

        readerTasks.add(midReader);
        threads.add(new Thread(midReader));

        RemoveTask removeTask = new RemoveTask(ceCache, readerTasks, endOffset);
        threads.add(new Thread(removeTask));

        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

//        System.out.println(ceCache.size());
//        System.out.println(reader1);
//        System.out.println(reader2);
//        System.out.println(reader3);
//        System.out.println(midReader);

        return reader1.getEvents();
    }

    private static class RemoveTask implements Runnable {
        private final EventCache<TestEvent<TestOffset>> cache;
        private final List<EventReaderTask<TestOffset, TestEvent<TestOffset>>> readers;
        private final long endOffset;

        public RemoveTask(EventCache<TestEvent<TestOffset>> cache,
                          List<EventReaderTask<TestOffset, TestEvent<TestOffset>>> readers,
                          long endOffset) {
            this.endOffset = endOffset;
            this.cache = cache;
            this.readers = readers;
        }

        @Override
        public void run() {
            boolean stop = false;
            do {

                if (readers.stream()
                        .mapToInt(r -> r.getEvents().size())
                        .allMatch(r -> r > 10000)) {

                    Optional<TestOffset> last = readers.stream()
                            .map(EventReaderTask::getLastReadKey)
                            .sorted()
                            .findFirst();

                    if (last.isPresent()) {
                        stop = readers.stream()
                                .map(EventReaderTask::getLastReadKey)
                                .allMatch(to -> last.get().equals(to) && last.get().offset() == endOffset);
                        if (stop) {
//                            System.out.println(last.get());
                        }
                        cache.remove(last.get());
                    }
                }
            } while (!stop); // The last event stays as there are no more events coming through
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);

//        EventCacheBenchmark benchmark = new EventCacheBenchmark();
//        benchmark.eventsCount = 20_000_000;
//        benchmark.rangeSize = 100_000;
//        benchmark.segmentSize = 2_000;

//            List<TestEvent<TestOffset>> events = benchmark.testEventReaderWithConcurrentEventCache();
//            List<TestEvent<TestOffset>> events = benchmark.testEventReaderWithSimpleEventCache();
//            System.out.println(events.size());
    }
}
