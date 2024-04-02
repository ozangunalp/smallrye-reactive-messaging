package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.ack.SqsDeleteAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.ack.SqsNothingAckHandler;
import io.smallrye.reactive.messaging.providers.helpers.PausablePollingStream;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class SqsInboundChannel {

    private final String channel;
    private final SqsClient client;
    private final Context context;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String queueUrl;
    private final Flow.Publisher<? extends Message<?>> stream;
    private final ScheduledExecutorService requestExecutor;
    private final int waitTimeSeconds;
    private final int maxNumberOfMessages;
    private final SqsReceiveMessageRequestCustomizer customizer;
    private final ExecutorService deleteWorkerThread;
    private final long retries;

    public SqsInboundChannel(SqsConnectorIncomingConfiguration conf, Vertx vertx, SqsClient client, String queueUrl,
            SqsReceiveMessageRequestCustomizer customizer) {
        this.channel = conf.getChannel();
        this.retries = conf.getReceiveRequestRetries();
        this.client = client;
        this.queueUrl = queueUrl;
        this.context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.requestExecutor = Executors
                .newSingleThreadScheduledExecutor(r -> new Thread(r, "smallrye-aws-sqs-request-thread-" + channel));
        this.waitTimeSeconds = conf.getWaitTimeSeconds();
        this.maxNumberOfMessages = conf.getMaxNumberOfMessages();
        this.customizer = customizer;

        SqsAckHandler ackHandler;
        if (conf.getAckDelete()) {
            this.deleteWorkerThread = Executors
                    .newSingleThreadExecutor(r -> new Thread(r, "smallrye-aws-sqs-delete-thread-" + channel));
            ackHandler = new SqsDeleteAckHandler(client, queueUrl, deleteWorkerThread);
        } else {
            this.deleteWorkerThread = null;
            ackHandler = new SqsNothingAckHandler();
        }
        PausablePollingStream<List<software.amazon.awssdk.services.sqs.model.Message>, software.amazon.awssdk.services.sqs.model.Message> pollingStream = new PausablePollingStream<>(
                channel, request(null, 0), (messages, processor) -> {
                    if (messages != null) {
                        for (var message : messages) {
                            processor.onNext(message);
                        }
                    }
                }, requestExecutor, maxNumberOfMessages * 2, conf.getReceiveRequestPauseResume());
        this.stream = pollingStream.getStream()
                .emitOn(r -> context.runOnContext(r))
                .onItem().transform(message -> new SqsMessage(message, ackHandler));
    }

    public Uni<List<software.amazon.awssdk.services.sqs.model.Message>> request(String requestId, int retryCount) {
        return Uni.createFrom().item(() -> {
            var builder = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .waitTimeSeconds(waitTimeSeconds)
                    .maxNumberOfMessages(maxNumberOfMessages);
            if (requestId != null) {
                builder.receiveRequestAttemptId(requestId);
            }
            if (customizer != null) {
                customizer.customize(builder);
            }
            var response = client.receiveMessage(builder.build());
            var messages = response.messages();
            if (messages == null || messages.isEmpty()) {
                log.receivedEmptyMessage();
                return null;
            }
            if (log.isTraceEnabled()) {
                messages.forEach(m -> log.receivedMessage(m.body()));
            }
            return messages;
        }).runSubscriptionOn(requestExecutor)
                .onFailure(e -> e instanceof SqsException && ((SqsException) e).retryable())
                .recoverWithUni(e -> {
                    if (retryCount < retries) {
                        return request(((SqsException) e).requestId(), retryCount + 1);
                    } else {
                        return Uni.createFrom().failure(e);
                    }
                })
                .onFailure().recoverWithItem(e -> {
                    log.errorReceivingMessage(e.getMessage());
                    return null;
                });
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return stream;
    }

    public void close() {
        closed.set(true);
        requestExecutor.shutdown();

        if (deleteWorkerThread != null) {
            deleteWorkerThread.shutdown();
        }
    }
}
