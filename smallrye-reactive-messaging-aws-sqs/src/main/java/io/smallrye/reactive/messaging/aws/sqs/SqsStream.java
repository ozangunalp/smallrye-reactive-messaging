package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging.log;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.vertx.mutiny.core.Context;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

@SuppressWarnings("PublisherImplementation")
public class SqsStream extends AbstractMulti<Message> {
    private final SqsClient client;
    private final SqsConnectorIncomingConfiguration conf;
    private final SqsReceiveMessageRequestCustomizer customizer;
    private final ScheduledExecutorService requestExecutor;
    private final Context context;

    private final String queueUrl;
    private final String channel;
    private final int waitTimeSeconds;

    private final int maxNumberOfMessages;

    public SqsStream(SqsClient client,
            String queueUrl,
            SqsConnectorIncomingConfiguration conf,
            SqsReceiveMessageRequestCustomizer customizer,
            ScheduledExecutorService requestExecutor,
            Context context) {
        this.client = client;
        this.conf = conf;
        this.channel = conf.getChannel();
        this.waitTimeSeconds = conf.getWaitTimeSeconds();
        this.maxNumberOfMessages = conf.getMaxNumberOfMessages();
        this.queueUrl = queueUrl;
        this.customizer = customizer;
        this.requestExecutor = requestExecutor;
        this.context = context;
    }

    private final Set<SqsMessageSubscription> subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void subscribe(MultiSubscriber<? super Message> subscriber) {
        SqsMessageSubscription subscription = new SqsMessageSubscription(subscriber, (messages, q) -> q.addAll(messages));
        subscriptions.add(subscription);
        subscriber.onSubscribe(subscription);
    }

    public class SqsMessageSubscription implements Flow.Subscription {

        private static final int STATE_NEW = 0; // no request yet -- we start polling on the first request
        private static final int STATE_POLLING = 1;
        private static final int STATE_PAUSED = 2;
        private static final int STATE_CANCELLED = 3;

        private volatile MultiSubscriber<? super Message> downstream;
        private final boolean pauseResumeEnabled;

        /**
         * Current state: new (no request yet), polling, paused, cancelled
         */
        private final AtomicInteger state = new AtomicInteger(STATE_NEW);

        private final AtomicInteger wip = new AtomicInteger();
        /**
         * Stores the current downstream demands.
         */
        private final AtomicLong requested = new AtomicLong();

        /**
         * The polling uni to avoid re-assembling a Uni everytime.
         */
        private final Uni<List<Message>> pollUni;

        private final int maxQueueSize;
        private final int halfMaxQueueSize;
        private final Queue<Message> queue;
        private final long retries;

        private SqsMessageSubscription(MultiSubscriber<? super Message> subscriber,
                BiConsumer<Collection<Message>, Queue<Message>> enqueue) {
            this.downstream = subscriber;
            this.pauseResumeEnabled = true;
            this.maxQueueSize = conf.getMaxNumberOfMessages() * 2;
            this.halfMaxQueueSize = maxQueueSize / 2;
            this.queue = Queues.createSpscChunkedArrayQueue(maxQueueSize);
            this.retries = Long.MAX_VALUE;
            this.pollUni = request(null, 0)
                    .onItem().transform(messages -> {
                        if (messages == null) {
                            return null;
                        }
                        if (log.isTraceEnabled()) {
                            log.tracef("Adding %s messages to the queue", messages.size());
                        }
                        enqueue.accept(messages, queue);
                        return messages;
                    });
        }

        public Uni<List<Message>> request(String requestId, int retryCount) {
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
                if (state.get() == STATE_POLLING) {
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
                } else {
                    return null;
                }
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

        @Override
        public void request(long n) {
            if (n > 0) {
                boolean cancelled = state.get() == STATE_CANCELLED;
                if (!cancelled) {
                    Subscriptions.add(requested, n);
                    if (state.compareAndSet(STATE_NEW, STATE_POLLING)) {
                        poll();
                    } else {
                        dispatch();
                    }
                }
            } else {
                throw new IllegalArgumentException("Invalid request");
            }

        }

        private void poll() {
            int state = this.state.get();
            if (state == STATE_CANCELLED || state == STATE_NEW) {
                return;
            }

            if (pauseResumeEnabled) {
                pauseResume();
            }

            pollUni.subscribe().with(messages -> {
                if (messages == null) {
                    executeWithDelay(this::poll, Duration.ofMillis(2))
                            .subscribe().with(this::emptyConsumer, this::report);
                } else {
                    dispatch();
                    runOnRequestThread(this::poll)
                            .subscribe().with(this::emptyConsumer, this::report);
                }
            }, this::report);
        }

        Uni<Void> runOnRequestThread(Runnable acion) {
            return Uni.createFrom().voidItem().invoke(acion).runSubscriptionOn(requestExecutor);
        }

        Uni<Void> executeWithDelay(Runnable action, Duration delay) {
            return Uni.createFrom().emitter(e -> {
                requestExecutor.schedule(() -> {
                    try {
                        action.run();
                    } catch (Exception ex) {
                        e.fail(ex);
                        return;
                    }
                    e.complete(null);
                }, delay.toMillis(), TimeUnit.MILLISECONDS);
            });
        }

        private void pauseResume() {
            int size = queue.size();
            if (size >= maxQueueSize && state.compareAndSet(STATE_POLLING, STATE_PAUSED)) {
                log.pausingRequestingMessages(channel, size, maxQueueSize);
            } else if (size <= halfMaxQueueSize && state.compareAndSet(STATE_PAUSED, STATE_POLLING)) {
                log.resumingRequestingMessages(channel, size, halfMaxQueueSize);
            }
        }

        private <I> void emptyConsumer(I ignored) {
        }

        private void report(Throwable fail) {
            while (true) {
                int state = this.state.get();
                if (state == STATE_CANCELLED) {
                    break;
                }
                if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                    downstream.onFailure(fail);
                    break;
                }
            }
        }

        void dispatch() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            context.runOnContext(this::run);
        }

        private void run() {
            int missed = 1;
            final Queue<Message> q = queue;
            long emitted = 0;
            long requests = requested.get();
            for (;;) {
                if (isCancelled()) {
                    return;
                }

                while (emitted != requests) {
                    Message item = q.poll();

                    if (item == null || isCancelled()) {
                        break;
                    }

                    downstream.onItem(item);
                    emitted++;
                }

                requests = requested.addAndGet(-emitted);
                emitted = 0;

                int w = wip.get();
                if (missed == w) {
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                } else {
                    missed = w;
                }
            }
        }

        @Override
        public void cancel() {
            while (true) {
                int state = this.state.get();
                if (state == STATE_CANCELLED) {
                    break;
                }
                if (this.state.compareAndSet(state, STATE_CANCELLED)) {
                    if (wip.getAndIncrement() == 0) {
                        // nothing was currently dispatched, clearing the queue.
                        client.close();
                        queue.clear();
                        downstream = null;
                    }
                    break;
                }
            }
        }

        boolean isCancelled() {
            if (state.get() == STATE_CANCELLED) {
                queue.clear();
                downstream = null;
                return true;
            }
            return false;
        }

    }

}
