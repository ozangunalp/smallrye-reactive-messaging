package io.smallrye.reactive.messaging.providers.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.reactivex.subscribers.TestSubscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class MultiConcatMapOpTest {

    AtomicInteger latestReceivedItemOffset;
    AtomicInteger upstreamRequestCount;
    Multi<Integer> upstream;

    @BeforeEach
    void setUp() {
        latestReceivedItemOffset = new AtomicInteger();
        upstreamRequestCount = new AtomicInteger();
        upstream = Multi.createFrom().generator(() -> upstreamRequestCount, (counter, emitter) -> {
            int requestCount = counter.getAndIncrement();
            assertThat(latestReceivedItemOffset).hasValue(requestCount);
            System.out.println("Upstream request " + requestCount + " " + latestReceivedItemOffset.get());
            emitter.emit(requestCount);
            return counter;
        });
    }

    @Test
    void flatmap_uni() {
        Multi<Integer> result = upstream.onItem().transformToUniAndConcatenate(integer -> {
            return Uni.createFrom().item(integer)
                    .onItem().delayIt().by(Duration.ofMillis(100))
                    .onItem().invoke(() -> latestReceivedItemOffset.getAndIncrement());
        });
        TestSubscriber<Integer> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.request(10);
        ts.awaitCount(20);
        ts.assertValueCount(20);
        assertThat(upstreamRequestCount).hasValue(20);
        System.out.println(ts.values());

        ts.request(1);
        ts.awaitCount(21);
        ts.assertValueCount(21);
        assertThat(upstreamRequestCount).hasValue(21);
        System.out.println(ts.values());
        ts.request(1);
        ts.awaitCount(22);
        ts.assertValueCount(22);
        assertThat(upstreamRequestCount).hasValue(22);
        System.out.println(ts.values());
    }

    @Test
    void flatmap_multi() {
        Multi<String> result = upstream.onItem().transformToMultiAndConcatenate(m -> {
            String s = Integer.toString(m);
            String s2 = Integer.toString(m * 2);
            return Multi.createFrom().items(s + "-" + s, s + "-" + s2)
                    .onCompletion().invoke(() -> latestReceivedItemOffset.getAndIncrement());
        });
        TestSubscriber<String> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.request(10);
        ts.awaitCount(20);
        ts.assertValueCount(20);
        assertThat(upstreamRequestCount).hasValue(20);
        System.out.println(ts.values());

        ts.request(1);
        ts.awaitCount(21);
        ts.assertValueCount(21);
        assertThat(upstreamRequestCount).hasValue(21);
        System.out.println(ts.values());
        ts.request(1);
        ts.awaitCount(22);
        ts.assertValueCount(22);
        assertThat(upstreamRequestCount).hasValue(22);
        System.out.println(ts.values());
    }

    @Test
    void concat_multi() {
        Multi<String> result = Infrastructure.onMultiCreation(new MultiConcatMapOp<>(upstream, m -> {
            String s = Integer.toString(m);
            String s2 = Integer.toString(m * 2);
            return Multi.createFrom().items(s + "-" + s, s + "-" + s2)
                    .onCompletion().invoke(() -> latestReceivedItemOffset.getAndIncrement());
        }, false));
        TestSubscriber<String> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.request(10);
        ts.awaitCount(20);
        assertThat(upstreamRequestCount).hasValue(10);
        ts.assertValueCount(20);
        System.out.println(ts.values());

        ts.request(1);
        assertThat(upstreamRequestCount).hasValue(11);
        ts.awaitCount(21);
        ts.assertValueCount(21);
        System.out.println(ts.values());
        ts.request(1);
        ts.awaitCount(22);
        ts.assertValueCount(22);
        assertThat(upstreamRequestCount).hasValue(11);
        System.out.println(ts.values());
    }

    @Test
    void concat_uni() {
        Multi<String> result = Infrastructure.onMultiCreation(new MultiConcatMapOp<>(upstream, m -> {
            return Uni.createFrom().item(m)
                    .onItem().delayIt().by(Duration.ofMillis(100))
                    .onItem().transform(i -> Integer.toString(i))
                    .toMulti()
                    .onCompletion().invoke(() -> latestReceivedItemOffset.getAndIncrement());
        }, false));
        TestSubscriber<String> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.request(10);
        ts.awaitCount(20, () -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 5000);
        ts.assertValueCount(20);
        assertThat(upstreamRequestCount).hasValue(20);

        ts.request(1);
        assertThat(upstreamRequestCount).hasValue(21);
        ts.awaitCount(21);
        ts.assertValueCount(21);
        System.out.println(ts.values());
        ts.request(1);
        ts.awaitCount(22);
        ts.assertValueCount(22);
        assertThat(upstreamRequestCount).hasValue(22);
        System.out.println(ts.values());
    }

    @Test
    void concat_empty() throws InterruptedException {
        Multi<String> result = Infrastructure.onMultiCreation(new MultiConcatMapOp<>(upstream, m -> {
            return Multi.createFrom().empty();
        }, false));
        TestSubscriber<String> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.request(10);
        ts.awaitCount(20);
        ts.assertNoValues();
        System.out.println(ts.values());
    }

    @Test
    void concat_prefetch() {
        AtomicBoolean firstCompleted = new AtomicBoolean(false);
        Multi<Integer> upstream = Multi.createFrom().generator(() -> 0, (i, e) -> {
            upstreamRequestCount.incrementAndGet();
            switch (i) {
                case 0:
                    e.emit(1);
                    return 1;
                case 1:
                    assertThat(firstCompleted).isTrue();
                    e.emit(2);
                    return 2;
                default:
                    e.complete();
                    return -1;
            }
        });
        Multi<Integer> result = Infrastructure.onMultiCreation(new MultiConcatMapOp<>(upstream, m -> {
            switch (m) {
                case 1:
                    return Uni.createFrom().item(m).onItem().delayIt().by(Duration.ofMillis(50))
                            .onItem().invoke(() -> firstCompleted.set(true))
                            .onItem().invoke(() -> assertThat(upstreamRequestCount).hasValue(m))
                            .toMulti();
                default:
                    return Uni.createFrom().item(m).toMulti();
            }
        }, false));
        TestSubscriber<Integer> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.awaitCount(2);
        ts.assertValues(1, 2);
        System.out.println(ts.values());
        ts.awaitTerminalEvent();
        ts.assertComplete();
    }

    @Test
    void concat_reactor_no_prefetch() {
        AtomicBoolean firstCompleted = new AtomicBoolean(false);
        Flux<Integer> fluxUpstream = Flux.generate(() -> upstreamRequestCount, (counter, emitter) -> {
            int requestCount = counter.getAndIncrement();
            assertThat(latestReceivedItemOffset).hasValue(requestCount);
            System.out.println("Upstream request " + requestCount + " " + latestReceivedItemOffset.get());
            emitter.next(requestCount);
            return counter;
        });
        Flux<Integer> result = fluxUpstream.concatMap(
                integer -> {
                    return Mono.delay(Duration.ofMillis(100))
                            .doOnNext(l -> latestReceivedItemOffset.getAndIncrement())
                            .map(l -> integer);
                },
                0);
        TestSubscriber<Integer> ts = new TestSubscriber<>(10);
        result.subscribe(ts);
        ts.request(10);
        ts.awaitCount(20);
        ts.assertValueCount(20);
        assertThat(upstreamRequestCount).hasValue(20);
        System.out.println(ts.values());

        ts.request(1);
        ts.awaitCount(21);
        ts.assertValueCount(21);
        assertThat(upstreamRequestCount).hasValue(21);
        System.out.println(ts.values());
        ts.request(1);
        ts.awaitCount(22);
        ts.assertValueCount(22);
        assertThat(upstreamRequestCount).hasValue(22);
        System.out.println(ts.values());

        assertThat(firstCompleted).isTrue();
    }
}
