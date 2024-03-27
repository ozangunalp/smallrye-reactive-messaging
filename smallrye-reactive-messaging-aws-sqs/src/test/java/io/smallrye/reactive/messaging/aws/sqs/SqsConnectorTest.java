package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

class SqsConnectorTest extends SqsTestBase {

    @Test
    void testConsumer() {
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.endpointOverride", localstack.getEndpoint().toString())
                .with("mp.messaging.incoming.data.region", localstack.getRegion())
                .with("mp.messaging.incoming.data.credentialsProvider",
                        "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @Test
    void testProducer() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.queue", queue)
                .with("mp.messaging.outgoing.data.endpointOverride", localstack.getEndpoint().toString())
                .with("mp.messaging.outgoing.data.region", localstack.getRegion());

        String queueUrl = createQueue(queue);
        var app = runApplication(config, ProducerApp.class);
        int expected = 10;
        List<Message> messages = receiveMessages(queueUrl, expected, Duration.ofSeconds(10));
        assertThat(messages).hasSize(expected);
    }
}
