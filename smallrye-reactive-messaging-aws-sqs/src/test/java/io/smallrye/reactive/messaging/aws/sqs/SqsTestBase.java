package io.smallrye.reactive.messaging.aws.sqs;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsTestBase extends WeldTestBase {

    DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:3.1.0");

    @Rule
    public LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.SQS);

    private SqsClient client;

    protected String queue;

    @BeforeEach
    void setupLocalstack(TestInfo testInfo) {
        localstack.start();
        System.setProperty("aws.accessKeyId", localstack.getAccessKey());
        System.setProperty("aws.secretAccessKey", localstack.getSecretKey());
        String cn = testInfo.getTestClass().map(Class::getSimpleName).orElse(UUID.randomUUID().toString());
        String mn = testInfo.getTestMethod().map(Method::getName).orElse(UUID.randomUUID().toString());
        queue = cn + "-" + mn + "-" + UUID.randomUUID().getMostSignificantBits();
    }

    @AfterEach
    public void stopContainer() {
        if (container != null) {
            var connector = getBeanManager().createInstance()
                    .select(SqsConnector.class, ConnectorLiteral.of(SqsConnector.CONNECTOR_NAME))
                    .get();
            connector.terminate(null);
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
        if (client != null) {
            client.close();
        }
        if (localstack.isRunning() || localstack.isCreated()) {
            localstack.close();
        }
    }

    public String sendMessage(String queueUrl, int numberOfMessages,
            BiConsumer<Integer, SendMessageRequest.Builder> messageProvider) {
        var sqsClient = getSqsClient();
        for (int i = 0; i < numberOfMessages; i++) {
            int j = i;
            sqsClient.sendMessage(r -> messageProvider.accept(j, r.queueUrl(queueUrl)));
        }
        return queueUrl;
    }

    public String sendMessage(String queueUrl, int numberOfMessages) {
        return sendMessage(queueUrl, numberOfMessages, (i, b) -> b.messageBody("hello"));
    }

    public synchronized SqsClient getSqsClient() {
        if (client == null) {
            client = SqsClient.builder().endpointOverride(localstack.getEndpoint())
                    .credentialsProvider(SystemPropertyCredentialsProvider.create())
                    .region(Region.of(localstack.getRegion()))
                    .build();
        }
        return client;
    }

    public List<Message> receiveMessages(String queueUrl,
            Consumer<ReceiveMessageRequest.Builder> receiveMessageRequest,
            Predicate<List<Message>> stopCondition) {
        var sqsClient = getSqsClient();
        var received = new CopyOnWriteArrayList<Message>();
        while (!stopCondition.test(received)) {
            received.addAll(
                    sqsClient.receiveMessage(r -> receiveMessageRequest.accept(r.queueUrl(queueUrl).maxNumberOfMessages(10)))
                            .messages());
        }
        return received;
    }

    public List<Message> receiveMessages(String queueUrl, Predicate<List<Message>> stopCondition) {
        return receiveMessages(queueUrl, r -> {
        }, stopCondition);
    }

    public List<Message> receiveMessages(String queueUrl, int numberOfMessages, Duration timeout) {
        Instant timeoutTs = Instant.now().plus(timeout);
        return receiveMessages(queueUrl, messages -> messages.size() >= numberOfMessages || Instant.now().isAfter(timeoutTs));
    }

    public List<Message> receiveMessages(String queueUrl, Consumer<ReceiveMessageRequest.Builder> receiveMessageRequest,
            int numberOfMessages, Duration timeout) {
        Instant timeoutTs = Instant.now().plus(timeout);
        return receiveMessages(queueUrl, receiveMessageRequest,
                messages -> messages.size() >= numberOfMessages || Instant.now().isAfter(timeoutTs));
    }

    public String createQueue(String queueName) {
        return getSqsClient().createQueue(r -> r.queueName(queueName)).queueUrl();
    }

    public String createQueue(String queueName, Consumer<CreateQueueRequest.Builder> requestConsumer) {
        return getSqsClient().createQueue(r -> requestConsumer.accept(r.queueName(queueName))).queueUrl();
    }
}
