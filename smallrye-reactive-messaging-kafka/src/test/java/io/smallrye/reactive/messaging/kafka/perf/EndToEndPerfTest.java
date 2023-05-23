package io.smallrye.reactive.messaging.kafka.perf;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.PerfTestUtils;
import io.smallrye.reactive.messaging.kafka.converters.RecordConverter;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * This test is intended to be used to generate flame-graphs to see where the time is spent in an end-to-end scenario.
 * It generates records to a topic.
 * Then, the application read from this topic and write to another one.
 * The test stops when an external consumers has received all the records written by the application.
 */
@Tag(TestTags.PERFORMANCE)
@Tag(TestTags.SLOW)
@Disabled
public class EndToEndPerfTest extends KafkaCompanionTestBase {

    public static final int COUNT = 50_000;
    public static String input_topic = UUID.randomUUID().toString();
    public static String output_topic = UUID.randomUUID().toString();

    @BeforeAll
    static void insertRecords() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(input_topic, "key", Long.toString(i)), COUNT)
                .awaitCompletion(Duration.ofMinutes(5));
    }

    private MapBasedConfig commonConfig() {
        return kafkaConfig("mp.messaging.incoming.in")
                .with("topic", input_topic)
                .with("pause-if-no-requests", true)
                .with("cloud-events", false)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .withPrefix("mp.messaging.outgoing.out")
                .with("topic", output_topic)
                .with("value.serializer", StringSerializer.class.getName())
                .with("key.serializer", StringSerializer.class.getName());
    }

    private void waitForOutMessages() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", companion.getBootstrapServers());
        properties.put("group.id", UUID.randomUUID().toString());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new StringDeserializer());
        consumer.subscribe(Collections.singletonList(output_topic));
        boolean done = false;
        long received = 0;
        while (!done) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            received = received + records.count();
            if (received == COUNT) {
                done = true;
            }
        }
    }

    @ApplicationScoped
    public static class MyNoopProcessor {
        @Incoming("in")
        @Outgoing("out")
        public Record<String, String> transform(Record<String, String> record) {
            return Record.of(record.key(), "hello-" + record.value());
        }

    }

    @ApplicationScoped
    public static class MyHardWorkerBlockingProcessor {
        @Incoming("in")
        @Outgoing("out")
        @Blocking
        public Record<String, String> transform(Record<String, String> record) {
            PerfTestUtils.consumeCPU(1_000_000);
            return Record.of(record.key(), "hello-" + record.value());
        }

    }

    @ApplicationScoped
    public static class MyHardWorkerProcessor {
        @Incoming("in")
        @Outgoing("out")
        public Uni<Message<String>> transform(Message<String> message) {
            return Uni.createFrom().item(message)
                    .onItem().invoke(() -> PerfTestUtils.consumeCPU(1_000_000))
                    .map(KafkaRecord::from);
        }

    }

    @Test
    public void test_noop_processor() {
        addBeans(RecordConverter.class);
        runApplication(commonConfig(), MyNoopProcessor.class);
        waitForOutMessages();
    }

    @Test
    public void test_hard_worker_blocking_processor() {
        addBeans(RecordConverter.class);
        runApplication(commonConfig(), MyHardWorkerBlockingProcessor.class);
        waitForOutMessages();
    }

    @Test
    public void test_hard_worker_processor() {
        addBeans(RecordConverter.class);
        runApplication(commonConfig(), MyHardWorkerProcessor.class);
        waitForOutMessages();
    }

}
