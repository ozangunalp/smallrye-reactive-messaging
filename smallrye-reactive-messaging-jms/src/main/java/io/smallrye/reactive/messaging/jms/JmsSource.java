package io.smallrye.reactive.messaging.jms;

import static io.smallrye.reactive.messaging.jms.i18n.JmsExceptions.ex;
import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.reactive.messaging.json.JsonMapping;

class JmsSource {

    private final Multi<IncomingJmsMessage<?>> source;
    private final JmsResourceHolder<JMSConsumer> resourceHolder;

    private final JmsPublisher publisher;

    JmsSource(JmsResourceHolder<JMSConsumer> resourceHolder, JmsConnectorIncomingConfiguration config, JsonMapping jsonMapping,
            Executor executor) {
        String channel = config.getChannel();
        final String destinationName = config.getDestination().orElseGet(config::getChannel);
        String selector = config.getSelector().orElse(null);
        boolean nolocal = config.getNoLocal();
        boolean durable = config.getDurable();
        String type = config.getDestinationType();
        boolean retry = config.getRetry();
        this.resourceHolder = resourceHolder.configure(r -> getDestination(r.getContext(), destinationName, type),
                r -> {
                    if (durable) {
                        if (!(r.getDestination() instanceof Topic)) {
                            throw ex.illegalArgumentInvalidDestination();
                        }
                        return r.getContext().createDurableConsumer((Topic) r.getDestination(), destinationName, selector,
                                nolocal);
                    } else {
                        return r.getContext().createConsumer(r.getDestination(), selector, nolocal);
                    }
                });
        resourceHolder.getClient();
        this.publisher = new JmsPublisher(resourceHolder);
        source = Multi.createFrom().publisher(publisher)
                .<IncomingJmsMessage<?>> map(m -> new IncomingJmsMessage<>(m, executor, jsonMapping))
                .onFailure(t -> {
                    log.terminalErrorOnChannel(channel);
                    this.resourceHolder.close();
                    return retry;
                })
                .retry()
                .withBackOff(Duration.parse(config.getRetryInitialDelay()),
                        Duration.parse(config.getRetryMaxDelay()))
                .withJitter(config.getRetryJitter())
                .atMost(config.getRetryMaxRetries())
                .onFailure()
                .invoke(throwable -> log.terminalErrorRetriesExhausted(config.getChannel(), throwable))
                .plug(m -> {
                    if (config.getBroadcast()) {
                        return m.broadcast().toAllSubscribers();
                    }
                    return m;
                });
    }

    void close() {
        publisher.close();
        resourceHolder.close();
    }

    private Destination getDestination(JMSContext context, String name, String type) {
        switch (type.toLowerCase()) {
            case "queue":
                log.creatingQueue(name);
                return context.createQueue(name);
            case "topic":
                log.creatingTopic(name);
                return context.createTopic(name);
            default:
                throw ex.illegalArgumentUnknownDestinationType(type);
        }

    }

    Multi<IncomingJmsMessage<?>> getSource() {
        return source;
    }

    @SuppressWarnings("PublisherImplementation")
    private static class JmsPublisher implements Flow.Publisher<Message>, Flow.Subscription {

        private final AtomicLong requests = new AtomicLong();
        private final AtomicReference<Flow.Subscriber<? super Message>> downstream = new AtomicReference<>();
        private final JmsResourceHolder<JMSConsumer> consumerHolder;
        private final ExecutorService executor;
        private boolean unbounded;

        private JmsPublisher(JmsResourceHolder<JMSConsumer> resourceHolder) {
            this.consumerHolder = resourceHolder;
            this.executor = Executors.newSingleThreadExecutor();
        }

        void close() {
            Flow.Subscriber<? super Message> subscriber = downstream.getAndSet(null);
            if (subscriber != null) {
                subscriber.onComplete();
            }
            executor.shutdown();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super Message> s) {
            if (downstream.compareAndSet(null, s)) {
                s.onSubscribe(this);
            } else {
                Subscriptions.fail(s, ex.illegalStateAlreadySubscriber());
            }
        }

        @Override
        public void request(long n) {
            if (n > 0) {
                boolean u = unbounded;
                if (!u) {
                    long v = add(n);
                    if (v == Long.MAX_VALUE) {
                        unbounded = true;
                        startUnboundedReception();
                    } else {
                        enqueue(n);
                    }
                }
            }
        }

        private void enqueue(long n) {
            for (int i = 0; i < n; i++) {
                executor.execute(() -> {
                    try {
                        Message message = null;
                        while (message == null && downstream.get() != null) {
                            message = consumerHolder.getClient().receive();
                            if (message != null) { // null means closed.
                                requests.decrementAndGet();
                                downstream.get().onNext(message);
                            }
                        }
                    } catch (JMSRuntimeException e) {
                        log.clientClosed();
                        Flow.Subscriber<? super Message> subscriber = downstream.getAndSet(null);
                        if (subscriber != null) {
                            subscriber.onError(e);
                        }
                    }
                });
            }
        }

        private void startUnboundedReception() {
            consumerHolder.getClient().setMessageListener(m -> downstream.get().onNext(m));
        }

        @Override
        public void cancel() {
            close();
        }

        long add(long req) {
            for (;;) {
                long r = requests.get();
                if (r == Long.MAX_VALUE) {
                    return Long.MAX_VALUE;
                }
                long u = r + req;
                long v;
                if (u < 0L) {
                    v = Long.MAX_VALUE;
                } else {
                    v = u;
                }
                if (requests.compareAndSet(r, v)) {
                    return v;
                }
            }
        }
    }
}
