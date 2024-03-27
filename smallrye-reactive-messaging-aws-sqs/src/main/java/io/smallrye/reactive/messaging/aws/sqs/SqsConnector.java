package io.smallrye.reactive.messaging.aws.sqs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(SqsConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "queue", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The name of the SQS queue, defaults to channel name if not provided")
@ConnectorAttribute(name = "region", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The name of the SQS region")
@ConnectorAttribute(name = "endpointOverride", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The endpoint override")
@ConnectorAttribute(name = "credentialsProvider", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The credential provider to be used in the client")
@ConnectorAttribute(name = "waitTimeSeconds", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum amount of time in seconds to wait for messages to be received", defaultValue = "1")
@ConnectorAttribute(name = "maxNumberOfMessages", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "The maximum number of messages to receive", defaultValue = "10")
@ConnectorAttribute(name = "receiveRequestCustomizer", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "The identifier for the bean implementing a customizer to receive requests, defaults to channel name if not provided")
@ConnectorAttribute(name = "ack.delete", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether the acknowledgement deletes the message from the queue", defaultValue = "true")
public class SqsConnector implements InboundConnector, OutboundConnector {

    @Inject
    private SqsManager sqsManager;

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    @Any
    Instance<SqsReceiveMessageRequestCustomizer> customizers;

    Vertx vertx;

    private static final List<SqsInboundChannel> INBOUND_CHANNELS = new CopyOnWriteArrayList<>();
    private static final List<SqsOutboundChannel> OUTBOUND_CHANNELS = new CopyOnWriteArrayList<>();
    public static final String CONNECTOR_NAME = "smallrye-sqs";

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        INBOUND_CHANNELS.forEach(SqsInboundChannel::close);
        OUTBOUND_CHANNELS.forEach(SqsOutboundChannel::close);
    }

    @Override
    public Publisher<? extends Message<?>> getPublisher(Config config) {
        var conf = new SqsConnectorIncomingConfiguration(config);
        var customizer = CDIUtils.getInstanceById(customizers, conf.getReceiveRequestCustomizer().orElse(conf.getChannel()),
                () -> null);
        var channel = new SqsInboundChannel(conf, vertx, sqsManager.getClient(conf), sqsManager.getQueueUrl(conf), customizer);
        INBOUND_CHANNELS.add(channel);
        return channel.getStream();
    }

    @Override
    public Subscriber<? extends Message<?>> getSubscriber(Config config) {
        var conf = new SqsConnectorOutgoingConfiguration(config);
        var channel = new SqsOutboundChannel(conf, sqsManager.getClient(conf), sqsManager.getQueueUrl(conf));
        OUTBOUND_CHANNELS.add(channel);
        return channel.getSubscriber();
    }
}
