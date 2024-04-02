package io.smallrye.reactive.messaging.aws.sqs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.aws.sqs.ack.SqsDeleteAckHandler;
import io.smallrye.reactive.messaging.aws.sqs.ack.SqsNothingAckHandler;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import software.amazon.awssdk.services.sqs.SqsClient;

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

    public SqsInboundChannel(SqsConnectorIncomingConfiguration conf, Vertx vertx, SqsClient client, String queueUrl,
            SqsReceiveMessageRequestCustomizer customizer) {
        this.channel = conf.getChannel();
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
        this.stream = new SqsStream(client, queueUrl, conf, customizer, requestExecutor, context)
                .onItem().transform(message -> new SqsMessage(message, ackHandler));
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
