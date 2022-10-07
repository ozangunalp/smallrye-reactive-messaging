package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

public class KafkaRestartTest {

    @Test
    void testRestartedBroker() {
        try (KafkaNativeContainer kafkaContainer = KafkaBrokerExtension.createKafkaContainer()) {
            kafkaContainer.start();
            await().until(kafkaContainer::isRunning);
            String bootstrapServers = kafkaContainer.getBootstrapServers();
            try (KafkaCompanion companion = new KafkaCompanion(bootstrapServers)) {
                companion.produceStrings()
                        .fromRecords(new ProducerRecord<>("topic", "1"))
                        .awaitCompletion();

                try (KafkaNativeContainer restarted = KafkaBrokerExtension.restart(kafkaContainer, 2)) {
                    assertThat(restarted.getBootstrapServers()).isEqualTo(bootstrapServers);

                    companion.produceStrings()
                            .fromRecords(new ProducerRecord<>("topic", "1"))
                            .awaitCompletion();
                }
            }
        }
    }
}
