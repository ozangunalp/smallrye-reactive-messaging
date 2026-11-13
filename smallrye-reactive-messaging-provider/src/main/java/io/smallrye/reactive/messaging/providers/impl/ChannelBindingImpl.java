package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.ChannelBinding;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;

class ChannelBindingImpl<I, O> implements ChannelBinding<I, O> {

    private final Multi<? extends Message<?>> upstream;
    private final ChannelFactory channelFactory;
    private final Executor executor;
    private final int concurrency;
    private final BiFunction<I, Metadata, Uni<O>> processor;

    ChannelBindingImpl(Multi<? extends Message<?>> upstream, ChannelFactory channelFactory) {
        this(upstream, channelFactory, null, 1, null);
    }

    private ChannelBindingImpl(Multi<? extends Message<?>> upstream, ChannelFactory channelFactory,
            Executor executor, int concurrency, BiFunction<I, Metadata, Uni<O>> processor) {
        this.upstream = upstream;
        this.channelFactory = channelFactory;
        this.executor = executor;
        this.concurrency = concurrency;
        this.processor = processor;
    }

    private Multi<? extends Message<?>> applyPipeline() {
        if (processor == null) {
            // Nothing to process: only move the delivery to the executor, if any.
            return executor != null ? upstream.emitOn(executor) : upstream;
        }
        Function<Message<?>, Multi<? extends Message<?>>> processMessage = this::processMessage;
        return upstream.plug(m -> {
            if (concurrency > 1) {
                return m.onItem().transformToMulti(processMessage).merge(concurrency);
            } else {
                return m.onItem().transformToMulti(processMessage).concatenate();
            }
        });
    }

    @SuppressWarnings("unchecked")
    private Multi<? extends Message<?>> processMessage(Message<?> msg) {
        I payload = (I) msg.getPayload();
        // Deferred so the processor runs when the inner stream is subscribed, not while the upstream emits.
        // Exceptions thrown by the processor become failures and nack the message.
        Uni<O> processing = Uni.createFrom().deferred(() -> processor.apply(payload, msg.getMetadata()));
        if (executor != null) {
            // Each message is processed on the executor, so merge(concurrency) runs up to `concurrency` at once,
            // and concatenate() keeps them sequential and ordered.
            processing = processing.runSubscriptionOn(executor);
        }
        return Multi.createFrom().uni(processing.onItem().transform(msg::withPayload))
                .onFailure().recoverWithMulti(e -> Multi.createFrom()
                        .completionStage(msg.nack(e))
                        .onItem().transformToMultiAndConcatenate(v -> Multi.createFrom().empty()));
    }

    @SuppressWarnings("unchecked")
    private ChannelBindingImpl<I, Void> processAndConsume(BiFunction<O, Metadata, Uni<Void>> consumer) {
        BiFunction<I, Metadata, Uni<Void>> composed;
        if (processor == null) {
            composed = (BiFunction<I, Metadata, Uni<Void>>) consumer;
        } else {
            composed = (payload, metadata) -> processor.apply(payload, metadata)
                    .onItem().transformToUni(result -> consumer.apply(result, metadata));
        }
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency, composed);
    }

    @Override
    public ChannelBinding<I, O> blocking() {
        return blocking(Infrastructure.getDefaultExecutor(), 1);
    }

    @Override
    public ChannelBinding<I, O> blocking(int concurrency) {
        return blocking(Infrastructure.getDefaultExecutor(), concurrency);
    }

    @Override
    public ChannelBinding<I, O> blocking(Executor executor) {
        return blocking(executor, 1);
    }

    @Override
    public ChannelBinding<I, O> blocking(Executor executor, int concurrency) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency, processor);
    }

    @Override
    public <T> ChannelBinding<I, T> process(Function<I, T> proc) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency,
                (payload, metadata) -> Uni.createFrom().item(proc.apply(payload)));
    }

    @Override
    public <T> ChannelBinding<I, T> process(BiFunction<I, Metadata, T> processor) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency,
                (payload, meta) -> Uni.createFrom().item(processor.apply(payload, meta)));
    }

    @Override
    public <T> ChannelBinding<I, T> processAsync(Function<I, Uni<T>> processor) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency,
                (payload, metadata) -> processor.apply(payload));
    }

    @Override
    public <T> ChannelBinding<I, T> processAsync(BiFunction<I, Metadata, Uni<T>> processor) {
        return new ChannelBindingImpl<>(upstream, channelFactory, executor, concurrency, processor);
    }

    @Override
    public Cancellable subscribe() {
        return applyPipeline()
                .onItem().call(msg -> Uni.createFrom().completionStage(msg.ack()))
                .subscribe().with(v -> {
                }, log::channelBindingSubscriptionError);
    }

    // Terminal operations

    @Override
    public Cancellable subscribe(Consumer<O> consumer) {
        return processAndConsume((payload, metadata) -> {
            consumer.accept(payload);
            return Uni.createFrom().voidItem();
        }).subscribe();
    }

    @Override
    public Cancellable subscribe(BiConsumer<O, Metadata> consumer) {
        return processAndConsume((payload, metadata) -> {
            consumer.accept(payload, metadata);
            return Uni.createFrom().voidItem();
        }).subscribe();
    }

    @Override
    public Cancellable subscribe(Function<O, Uni<Void>> consumer) {
        return processAndConsume((payload, metadata) -> consumer.apply(payload)).subscribe();
    }

    @Override
    public Cancellable subscribe(BiFunction<O, Metadata, Uni<Void>> consumer) {
        return processAndConsume(consumer).subscribe();
    }

    @Override
    public Cancellable to(OutgoingConnectorConfiguration config) {
        return to(channelFactory.outgoing(config));
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public Cancellable to(Flow.Subscriber<? extends Message<?>> subscriber) {
        CompletingOnCancelSubscriber s = new CompletingOnCancelSubscriber((Flow.Subscriber<Message<?>>) subscriber);
        Cancellable cancellable = applyPipeline().subscribe()
                .with(s.downstream::onSubscribe, s::onNext, s::onError, s::onComplete);
        return () -> {
            cancellable.cancel();
            // Cancelling the binding must not leave the connector sink open.
            s.onComplete();
        };
    }

    /**
     * Forwards signals to the downstream subscriber, guaranteeing a single terminal signal and serial delivery even
     * when completion comes from a cancellation racing with an in-flight {@code onNext}.
     */
    private static final class CompletingOnCancelSubscriber {

        private final Flow.Subscriber<Message<?>> downstream;
        private boolean terminated;

        CompletingOnCancelSubscriber(Flow.Subscriber<Message<?>> downstream) {
            this.downstream = downstream;
        }

        synchronized void onNext(Message<?> item) {
            if (!terminated) {
                downstream.onNext(item);
            }
        }

        synchronized void onError(Throwable failure) {
            if (!terminated) {
                terminated = true;
                downstream.onError(failure);
            }
        }

        synchronized void onComplete() {
            if (!terminated) {
                terminated = true;
                downstream.onComplete();
            }
        }
    }

}
