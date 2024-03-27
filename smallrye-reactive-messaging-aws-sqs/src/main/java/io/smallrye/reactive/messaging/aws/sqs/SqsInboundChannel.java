package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.ack.SqsDeleteAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.ack.SqsNothingAckHandler;
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
    private final ExecutorService requestExecutor;
    private final int waitTimeSeconds;
    private final int maxNumberOfMessages;
    private final SqsReceiveMessageRequestCustomizer customizer;

    public SqsInboundChannel(SqsConnectorIncomingConfiguration conf, Vertx vertx, SqsClient client, String queueUrl,
                             SqsReceiveMessageRequestCustomizer customizer) {
        this.channel = conf.getChannel();
        this.client = client;
        this.queueUrl = queueUrl;
        this.context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.requestExecutor = Executors
                .newSingleThreadExecutor(r -> new Thread(r, "smallrye-aws-sqs-request-thread-" + channel));
        this.waitTimeSeconds = conf.getWaitTimeSeconds();
        this.maxNumberOfMessages = conf.getMaxNumberOfMessages();
        this.customizer = customizer;
        SqsAckHandler ackHandler = conf.getAckDelete() ? new SqsDeleteAckHandler(client, queueUrl, requestExecutor)
                : new SqsNothingAckHandler();
        this.stream = Multi.createBy().repeating()
                .uni(() -> request(null))
                .until(__ -> closed.get())
                .onItem().transformToIterable(Function.identity())
                .emitOn(context::runOnContext)
                .onItem().transform(message -> new SqsMessage(message, ackHandler));
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return stream;
    }

    public Uni<List<software.amazon.awssdk.services.sqs.model.Message>> request(String requestId) {
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
                    if (!closed.get()) {
                        var response = client.receiveMessage(builder.build());
                        var messages = response.messages();
                        if (messages == null || messages.isEmpty()) {
                            log.receivedEmptyMessage();
                            System.out.println("Received empty message");
                            return null;
                        }
                        if (log.isTraceEnabled()) {
                            messages.forEach(m -> log.receivedMessage(m.body()));
                        }
                        return messages;
                    }
                    return null;
                })
                .onFailure(e -> e instanceof SqsException && ((SqsException) e).retryable())
                .recoverWithUni(e -> request(((SqsException) e).requestId()))
                .onFailure().recoverWithItem(e -> {
                    log.errorReceivingMessage(e.getMessage());
                    return null;
                })
                .runSubscriptionOn(requestExecutor);
    }

    public void close() {
        closed.set(true);
        requestExecutor.shutdown();
        System.out.println("Closed channel " + channel);
    }
}
