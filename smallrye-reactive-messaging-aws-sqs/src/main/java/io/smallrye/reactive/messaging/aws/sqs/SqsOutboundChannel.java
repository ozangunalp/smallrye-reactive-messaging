package io.smallrye.reactive.messaging.aws.sqs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsOutboundChannel {

    private Flow.Subscriber<? extends Message<?>> subscriber;
    private final SqsClient client;

    private final String queueUrl;

    public SqsOutboundChannel(SqsClient client, String queueUrl) {
        this.client = client;
        this.subscriber = MultiUtils.via(multi -> multi.call(m -> publishMessage(this.client, m)));
        this.queueUrl = queueUrl;
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    private Uni<Void> publishMessage(SqsClient client, Message<?> m) {
        var sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl)
                .messageBody(m.getPayload().toString())
                .build();
        return Uni.createFrom().completionStage(CompletableFuture.runAsync(() -> client.sendMessage(sendMessageRequest)))
                .onItem().transformToUni(receipt -> Uni.createFrom().completionStage(m.ack()))
                .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(m.nack(t)));
    }
}
