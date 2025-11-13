package io.smallrye.reactive.messaging.kafka.impl;

import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.TopicPartitionKey;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

final class TopicPartitionKeyQueue<K, V> {
    private final TopicPartitionKey key;
    private final ConcurrentMap<TopicPartitionKey, TopicPartitionKeyQueue<K, V>> owner;
    private final Deque<IncomingKafkaRecord<K, V>> queue = new ArrayDeque<>();
    private boolean inFlight = false;

    TopicPartitionKeyQueue(TopicPartitionKey key, ConcurrentMap<TopicPartitionKey, TopicPartitionKeyQueue<K, V>> owner) {
        this.key = key;
        this.owner = owner;
    }

    void submit(IncomingKafkaRecord<K, V> rec,
            MultiEmitter<? super IncomingKafkaRecord<K, V>> emitter) {
        if (!inFlight) {
            emit(rec, emitter);
        } else {
            queue.addLast(rec);
        }
    }

    private void emit(IncomingKafkaRecord<K, V> rec,
            MultiEmitter<? super IncomingKafkaRecord<K, V>> emitter) {
        inFlight = true;
        rec.afterProcessing(() -> emitNextIfAny(emitter));
        emitter.emit(rec);
    }

    private void emitNextIfAny(MultiEmitter<? super IncomingKafkaRecord<K, V>> emitter) {
        final AtomicReference<IncomingKafkaRecord<K, V>> nextRecordHolder = new AtomicReference<>();

        owner.compute(key, (k, queue) -> {
            if (queue != this) {
                return queue;
            }

            IncomingKafkaRecord<K, V> next = this.queue.pollFirst();
            nextRecordHolder.set(next);

            if (next == null) {
                inFlight = false;
                return null;
            } else {
                return this;
            }
        });

        IncomingKafkaRecord<K, V> next = nextRecordHolder.get();
        if (next != null) {
            emit(next, emitter);
        }
    }
}
