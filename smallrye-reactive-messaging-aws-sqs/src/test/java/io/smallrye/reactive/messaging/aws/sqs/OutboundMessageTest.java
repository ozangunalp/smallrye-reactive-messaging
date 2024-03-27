package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.GenericPayload;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class OutboundMessageTest extends SqsTestBase {

    @Test
    void testOutboundMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        String queueUrl = createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue);

        runApplication(config, RequestBuilderProducingApp.class);
        var received = receiveMessages(queueUrl, r -> r.messageAttributeNames("key"), expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(10)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testOutboundMetadataMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        String queueUrl = createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue);

        runApplication(config, OutgoingMetadataProducingApp.class);
        var received = receiveMessages(queueUrl, r -> r.messageAttributeNames("key"), expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(10)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        String queueSink = queue + "-sink.fifo";
        queue = queue + ".fifo";
        String queueUrl = createQueue(queue, r -> r.attributes(
                Map.of(QueueAttributeName.FIFO_QUEUE, "true",
                        QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup")));

        sendMessage(queueUrl, 10, (i, r) -> r.messageGroupId("group")
                .messageDeduplicationId("m-" + i)
                .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                        .dataType("String").stringValue("value").build()))
                .messageBody(String.valueOf(i)));

        String queueSinkUrl = createQueue(queueSink, r -> r.attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true",
                QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup")));
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queueSink);

        runApplication(config, MessageProducingApp.class);
        var received = receiveMessages(queueSinkUrl, r -> r.messageAttributeNames("key"), expected, Duration.ofMinutes(1));
        assertThat(received).hasSizeGreaterThan(10)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).contains("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @ApplicationScoped
    public static class RequestBuilderProducingApp {
        @Outgoing("sink")
        public Multi<SendMessageRequest.Builder> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> SendMessageRequest.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .messageBody(String.valueOf(i)));
        }

    }

    @ApplicationScoped
    public static class OutgoingMetadataProducingApp {
        @Outgoing("sink")
        public Multi<Message<String>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> Message.of(String.valueOf(i), Metadata.of(SqsOutboundMetadata.builder()
                            .setMessageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .build())));
        }

    }

    @ApplicationScoped
    public static class MessageProducingApp {
        @Incoming("data")
        @Outgoing("sink")
        public GenericPayload<software.amazon.awssdk.services.sqs.model.Message> process(
                software.amazon.awssdk.services.sqs.model.Message message) {
            return GenericPayload.of(message, Metadata.of(SqsOutboundMetadata.builder()
                    .setGroupId("group")
                    .setDeduplicationId(message.messageId())
                    .setMessageAttributes(Map.of("key", MessageAttributeValue.builder()
                            .dataType("String").stringValue("value").build()))
                    .build()));
        }

    }
}
