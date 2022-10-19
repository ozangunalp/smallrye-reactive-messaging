package io.smallrye.reactive.messaging.kafka.commit;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class FileCheckpointStateStoreTest extends KafkaCompanionTestBase {

    private KafkaSource<String, Integer> source;
    private KafkaSource<String, Integer> source2;

    @AfterEach
    public void stopAll() {
        if (source != null) {
            source.closeQuietly();
        }
        if (source2 != null) {
            source2.closeQuietly();
        }
    }

    private void checkOffsetSum(File tempDir, int sum) {
        await().until(() -> {
            List<JsonObject> states = Uni.join().all(Stream.of(0, 1, 2)
                    .map(i -> tempDir.toPath().resolve(topic + "-" + i).toString())
                    .map(path -> vertx.fileSystem().readFile(path)
                            .map(Buffer::toJsonObject)
                            .onFailure().recoverWithItem(JsonObject.of("offset", 0, "state", 0)))
                    .collect(Collectors.toList()))
                    .andFailFast()
                    .await().indefinitely();

            int offset = states.stream().mapToInt(tuple -> tuple.getInteger("offset")).sum();
            int state = states.stream().mapToInt(tuple -> tuple.getInteger("state")).sum();

            return offset == sum && state == sum(sum);
        });
    }

    private void checkOffsetSum(File tempDir, int partition, int sum) {
        await().until(() -> {
            JsonObject json = vertx.fileSystem().readFile(tempDir.toPath().resolve(topic + "-" + partition).toString())
                    .map(Buffer::toJsonObject)
                    .onFailure().recoverWithItem(JsonObject.of("offset", 0, "state", 0))
                    .await().indefinitely();

            int offset = json.getInteger("offset");
            int state = json.getInteger("state");

            return offset == sum && state == sum(sum);
        });
    }

    private int sum(int sum) {
        return sum * (sum - 1) / 2;
    }

    @Test
    public void testMultipleIndependentConsumers(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = newCommonConfigForSource()
                .with("group.id", "test-source-with-auto-commit-enabled")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);

        SingletonInstance<KafkaCommitHandler.Factory> checkpointFactory = new SingletonInstance<>("checkpoint",
                new KafkaCheckpointCommit.Factory(new SingletonInstance<>("file", new FileCheckpointStateStore.Factory())));
        source = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic,
                checkpointFactory,
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().onItem().transformToUniAndConcatenate(m -> {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(m);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + m.getPayload(), true);
            }
            messages.add(m);
            return Uni.createFrom().completionStage(m.ack());
        }).subscribe().with(x -> {

        });

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i), i), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() >= 100);
        checkOffsetSum(tempDir, 100);

        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(
                config.with(ConsumerConfig.CLIENT_ID_CONFIG,
                        source.getConsumer().get(ConsumerConfig.CLIENT_ID_CONFIG) + "-2"));
        source2 = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic2,
                checkpointFactory,
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().onItem().transformToUniAndConcatenate(m -> {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(m);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + m.getPayload(), true);
            }
            messages2.add(m);
            return Uni.createFrom().completionStage(m.ack());
        }).subscribe().with(x -> {

        });

        await().until(() -> !source2.getConsumer().getAssignments().await().indefinitely().isEmpty());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i + 100), i + 100), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 200);
        checkOffsetSum(tempDir, 200);

        source.closeQuietly();
        await().until(() -> source2.getConsumer().getAssignments().await().indefinitely().size() == 3);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i + 200), i + 200), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 300);
        checkOffsetSum(tempDir, 300);
    }

    @Test
    public void testWithPartitions(@TempDir File tempDir) {
        System.out.println(tempDir.getAbsolutePath());

        addBeans(FileCheckpointStateStore.Factory.class, KafkaCheckpointCommit.Factory.class);

        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        StoringBean application = runApplication(config, StoringBean.class);

        int expected = 1000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() == expected);

        checkOffsetSum(tempDir, expected);
    }

    @ApplicationScoped
    public static class StoringBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + msg.getPayload(), true);
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @Test
    public void testSelectiveCheckpoint(@TempDir File tempDir) {
        System.out.println(tempDir.getAbsolutePath());

        addBeans(FileCheckpointStateStore.Factory.class, KafkaCheckpointCommit.Factory.class);

        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        SelectiveCheckpointBean application = runApplication(config, SelectiveCheckpointBean.class);

        int expected = 500;
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, 0, "", i), expected)
                .awaitCompletion(Duration.ofMinutes(1));
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, 1, "", i), expected)
                .awaitCompletion(Duration.ofMinutes(1));
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, 2, "", i), expected)
                .awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() == expected * 3);

        checkOffsetSum(tempDir, 0, expected);
        checkOffsetSum(tempDir, 1, expected);
        checkOffsetSum(tempDir, 2, expected);
    }

    @ApplicationScoped
    public static class SelectiveCheckpointBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                IncomingKafkaRecordMetadata metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).get();
                if ((metadata.getOffset() + 1) % 10 == 0) {
                    checkpointMetadata.transform(0, current -> current + msg.getPayload(), true);
                } else {
                    checkpointMetadata.transform(0, current -> current + msg.getPayload());
                }
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

}
