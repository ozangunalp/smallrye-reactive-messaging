package io.smallrye.reactive.messaging.aws.sqs.ack;

import java.util.concurrent.Executor;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.SqsMessage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

public class SqsDeleteAckHandler implements SqsAckHandler {

    private final SqsClient client;
    private final String queueUrl;
    private final Executor deleteWorkerThread;

    public SqsDeleteAckHandler(SqsClient client, String queueUrl, Executor deleteWorkerThread) {
        this.client = client;
        this.queueUrl = queueUrl;
        this.deleteWorkerThread = deleteWorkerThread;
    }

    @Override
    public Uni<Void> handle(SqsMessage message) {
        DeleteMessageRequest build = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.getMessage().receiptHandle())
                .build();
        return Uni.createFrom().item(() -> client.deleteMessage(build))
                .runSubscriptionOn(deleteWorkerThread)
                .replaceWithVoid()
                .emitOn(message::runOnMessageContext);
    }
}
