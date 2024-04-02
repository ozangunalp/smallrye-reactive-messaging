package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.smallrye.reactive.messaging.json.jackson.JacksonMapping;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.Json;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

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
    void testConsumerInteger() {
        int expected = 10;
        sendMessage(createQueue(queue), expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(Integer.class.getName()).build())));
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.endpointOverride", localstack.getEndpoint().toString())
                .with("mp.messaging.incoming.data.region", localstack.getRegion())
                .with("mp.messaging.incoming.data.credentialsProvider",
                        "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");

        ConsumerIntegerApp app = runApplication(config, ConsumerIntegerApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @Test
    void testConsumerJson() {
        addBeans(JacksonMapping.class);
        addBeans(ObjectMapperProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected, (i, r) -> {
            ConsumerJsonApp.Person person = new ConsumerJsonApp.Person();
            person.name = "person-" + i;
            person.age = i;
            r.messageBody(Json.encode(person))
                    .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue(ConsumerJsonApp.Person.class.getName()).build()));
        });
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.endpointOverride", localstack.getEndpoint().toString())
                .with("mp.messaging.incoming.data.region", localstack.getRegion())
                .with("mp.messaging.incoming.data.credentialsProvider",
                        "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider");

        ConsumerJsonApp app = runApplication(config, ConsumerJsonApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class ObjectMapperProvider {
        @Produces
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
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
