package io.smallrye.reactive.messaging.kafka.commit;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.vertx.mutiny.core.Vertx;

/**
 *
 */
public interface StateStore {

    Uni<Map<TopicPartition, ProcessingState<?>>> fetchProcessingState(Collection<TopicPartition> partitions);

    Uni<Void> persistProcessingState(Map<TopicPartition, ProcessingState<?>> state);

    default void close() {
        // no implementation
    }

    interface Factory {
        StateStore create(KafkaConnectorIncomingConfiguration config, Vertx vertx);
    }
}
