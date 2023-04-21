package io.smallrye.reactive.messaging.kafka.companion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.RepeatedTest;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class TopicsTest extends KafkaCompanionTestBase {

    @RepeatedTest(30)
    void testCreateTopicAndWait() {
        String topicName = UUID.randomUUID().toString();
        companion.topics().createAndWait(topicName, 2);

        await().atMost(1, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(companion.topics().describe()).containsKey(topicName)
                        .extractingByKey(topicName)
                        .extracting(TopicDescription::partitions)
                        .satisfies(partitions -> assertThat(partitions).hasSize(2)));
    }

    @RepeatedTest(30)
    void testCreateTopic() {
        String newTopic = UUID.randomUUID().toString();
        companion.topics().create(newTopic, 1);
        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> companion.topics().list().contains(newTopic));
    }

    @RepeatedTest(30)
    void testDescribeTopics() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();
        Map<String, Integer> topics = new HashMap<>();
        topics.put(topic1, 3);
        topics.put(topic2, 2);
        topics.put(topic3, 1);
        companion.topics().create(topics);

        await().atMost(1, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(companion.topics().list()).contains(topic1, topic2, topic3));

        assertThat(companion.topics().describe()).containsKeys(topic1, topic2, topic3)
                .hasEntrySatisfying(topic1, t -> assertThat(t.partitions()).hasSize(3))
                .hasEntrySatisfying(topic2, t -> assertThat(t.partitions()).hasSize(2))
                .hasEntrySatisfying(topic3, t -> assertThat(t.partitions()).hasSize(1));
    }

    @RepeatedTest(30)
    void testCreateAndWaitAndDelete() {
        companion.topics().createAndWait(topic + "-new", 3);
        companion.topics().delete(topic + "-new");
        await().until(() -> !companion.topics().list().contains(topic + "-new"));
    }
}
