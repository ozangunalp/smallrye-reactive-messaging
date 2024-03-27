package io.smallrye.reactive.messaging.aws.sqs;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import software.amazon.awssdk.services.sqs.SqsClient;

@ApplicationScoped
public class SqsClientProvider {

    public static SqsClient client;

    @Produces
    public SqsClient createClient() {
        return client;
    }

}
