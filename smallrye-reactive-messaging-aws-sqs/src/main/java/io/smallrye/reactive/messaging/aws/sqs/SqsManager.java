package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsExceptions.ex;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import software.amazon.awssdk.services.sqs.SqsClient;

@ApplicationScoped
public class SqsManager {

    @Inject
    Instance<SqsClient> clientInstance;

    private final Map<SqsClientConfig, SqsClient> clients = new ConcurrentHashMap<>();

    private final Map<SqsClientConfig, String> queueUrls = new ConcurrentHashMap<>();

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        clients.values().forEach(SqsClient::close);
    }

    private SqsClient getClient(SqsClientConfig config) {
        return clients.computeIfAbsent(config, c -> {
            if (clientInstance.isResolvable() && !c.isComplete()) {
                return clientInstance.get();
            }
            var builder = SqsClient.builder();
            if (c.getEndpointOverride() != null) {
                builder.endpointOverride(URI.create(c.getEndpointOverride()));
            }
            if (c.getRegion() != null) {
                builder.region(c.getRegion());
            }
            builder.credentialsProvider(config.createCredentialsProvider());
            return builder.build();
        });

    }

    public SqsClient getClient(SqsConnectorCommonConfiguration config) {
        return getClient(new SqsClientConfig(config));
    }

    public String getQueueUrl(SqsConnectorCommonConfiguration config) {
        SqsClientConfig clientConfig = new SqsClientConfig(config);
        return queueUrls.computeIfAbsent(clientConfig, c -> {
            try {
                return getClient(clientConfig).getQueueUrl(r -> r.queueName(c.getQueueName()).build()).queueUrl();
            } catch (RuntimeException e) {
                throw ex.illegalStateUnableToRetrieveQueueUrl(e);
            }
        });
    }
}
