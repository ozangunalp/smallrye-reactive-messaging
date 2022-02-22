package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.vertx.mutiny.core.Vertx;

/**
 * Will commit the record offset received by the Kafka consumer (if higher than the previously committed offset).
 * This offset may be greater than the currently ACKed message.
 * <p>
 * This handler is the default when `enable.auto.commit` is `false`.
 * This strategy provides at-least-once delivery if the channel processes the message without performing
 * any asynchronous processing.
 * <p>
 * This strategy should not be used on high-load as offset commit is expensive.
 * <p>
 * To use set `commit-strategy` to `latest`.
 */
public class KafkaLatestCommit extends ContextHolder implements KafkaCommitHandler {

    private final ReactiveKafkaConsumer<?, ?> consumer;

    /**
     * Stores the last offset for each topic/partition.
     * This map must always be accessed from the same thread (Vert.x context).
     */
    private final Map<TopicPartition, Long> offsets = new HashMap<>();

    public KafkaLatestCommit(Vertx vertx, KafkaConnectorIncomingConfiguration configuration,
            ReactiveKafkaConsumer<?, ?> consumer) {
        super(vertx.getDelegate(), configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class).orElse(60000));
        this.consumer = consumer;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
        runOnRecordContext(record, () -> {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            TopicPartition key = new TopicPartition(record.getTopic(), record.getPartition());
            Long last = offsets.get(key);
            // Verify that the latest committed offset before this one.
            if (last == null || last < record.getOffset() + 1) {
                offsets.put(key, record.getOffset() + 1);
                map.put(key, new OffsetAndMetadata(record.getOffset() + 1, null));
                consumer.commitAsync(map)
                        .subscribe().with(ignored -> {
                        }, throwable -> log.failedToCommitAsync(key, record.getOffset() + 1));
            }
        });
        return CompletableFuture.completedFuture(null);
    }
}
