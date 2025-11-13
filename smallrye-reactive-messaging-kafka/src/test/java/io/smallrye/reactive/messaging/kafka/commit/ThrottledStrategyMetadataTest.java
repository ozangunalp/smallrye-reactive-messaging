package io.smallrye.reactive.messaging.kafka.commit;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ThrottledStrategyMetadataTest extends KafkaCompanionTestBase {

    @Test
    public void testMetadataSavingEvery2ndOutOfOrderMessage() throws InterruptedException {
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config1 = new MapBasedConfig()
                .with("channel-name", "data1")
                .with("bootstrap.servers", companion.getBootstrapServers())
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("max.poll.records", 4)
                .with("commit-strategy", "throttled")
                .with("auto.commit.interval.ms", 100)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "A")
                .with("throttled.unprocessed-record-max-age.ms", 100000)
                .with("key.deserializer", StringDeserializer.class.getName());

        MapBasedConfig config2 = new MapBasedConfig()
                .with("channel-name", "data2")
                .with("bootstrap.servers", companion.getBootstrapServers())
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("max.poll.records", 4)
                .with("commit-strategy", "throttled")
                .with("auto.commit.interval.ms", 100)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "B")
                .with("throttled.unprocessed-record-max-age.ms", 100000)
                .with("key.deserializer", StringDeserializer.class.getName());

        List<Integer> list1 = new ArrayList<>();

        List<Integer> list2 = new ArrayList<>();

        KafkaSource<String, Integer> source1 = new KafkaSource<>(vertx, groupId,
                new KafkaConnectorIncomingConfiguration(config1),
                UnsatisfiedInstance.instance(), commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance(), 0);

        KafkaSource<String, Integer> source2 = new KafkaSource<>(vertx, groupId,
                new KafkaConnectorIncomingConfiguration(config2),
                UnsatisfiedInstance.instance(), commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), UnsatisfiedInstance.instance(), 0);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 50)
                .awaitCompletion();

        AtomicInteger counter = new AtomicInteger(0);

        source1.getStream()
                .emitOn(Executors.newFixedThreadPool(50))
                .onItem()
                .transformToUniAndMerge(r -> {
                    int count = counter.getAndIncrement();
                    list1.add(r.getPayload());
                    if (count % 2 == 0) {
                        //complete if even
                        CompletableFuture.runAsync(r::ack);
                        return Uni.createFrom().voidItem();
                    }
                    //just hang otherwise
                    return Uni.createFrom().item(r)
                            .onItem().delayIt().by(Duration.ofHours(1))
                            .replaceWithVoid();
                })
                .subscribe().with(x -> {
                });

        await().until(() -> list1.size() == 50); //we got all items
        assertThat(list1).hasSameElementsAs(IntStream.range(0, 50).boxed().toList());

        Thread.sleep(200); //wait autocommit

        source1.closeQuietly();

        source2.getStream()
                .subscribe().with(r -> {
                    list2.add(r.getPayload());
                    CompletableFuture.runAsync(r::ack);
                });

        //since we already processed every 2nd element, this time we only process other unprocessed elements
        await().until(() -> list2.size() == 25);
        assertThat(list2).hasSameElementsAs(IntStream.range(0, 50).filter(i -> i % 2 != 0).boxed().toList());
    }
}
