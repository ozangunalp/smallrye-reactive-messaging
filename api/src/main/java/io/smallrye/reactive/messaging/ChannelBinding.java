package io.smallrye.reactive.messaging;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;

/**
 * A binding between a channel publisher and a typed payload consumer.
 * <p>
 * Created via {@link ChannelFactory#bind(java.util.concurrent.Flow.Publisher, Class)},
 * this provides a fluent API to subscribe to a channel with automatic payload conversion
 * and ack/nack handling.
 * <p>
 * On each message:
 * <ul>
 * <li>The payload is converted to the target type using available {@link MessageConverter}s</li>
 * <li>The consumer is invoked with the converted payload (and optionally the message {@link Metadata})</li>
 * <li>On success, the message is acknowledged</li>
 * <li>On failure, the message is negatively acknowledged with the exception</li>
 * </ul>
 *
 * Processing steps added with {@link #process(Function)}, {@link #processAsync(Function)} and their metadata variants
 * are chained: each step receives the output of the previous one, so {@code .process(a).process(b)} applies {@code a}
 * then {@code b}. A failure in any step negatively acknowledges the message.
 *
 * @param <I> the payload type of the channel
 * @param <O> the output type: the payload type, or the result type of the last processing step
 */
public interface ChannelBinding<I, O> {

    /**
     * Terminal operation that acknowledges each processed message and discards it.
     * The processing steps, if any, are applied before acknowledging.
     * On failure, the message is negatively acknowledged.
     *
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe();

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Adds the consumer as the last processing step and subscribes, acknowledging each message after
     * the consumer completes successfully.
     *
     * @param consumer the payload consumer
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(Consumer<O> consumer);

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Adds the consumer as the last processing step and subscribes, acknowledging each message after
     * the consumer completes successfully.
     *
     * @param consumer the payload and metadata consumer
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(BiConsumer<O, Metadata> consumer);

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Adds the consumer as the last processing step and subscribes, acknowledging each message after
     * the returned {@link Uni} completes.
     *
     * @param consumer the async payload consumer returning a {@link Uni}
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(Function<O, Uni<Void>> consumer);

    /**
     * Convenience for {@code .process(consumer).subscribe()}.
     * Adds the consumer as the last processing step and subscribes, acknowledging each message after
     * the returned {@link Uni} completes.
     *
     * @param consumer the async payload and metadata consumer returning a {@link Uni}
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable subscribe(BiFunction<O, Metadata, Uni<Void>> consumer);

    /**
     * Returns a new {@link ChannelBinding} that runs the processing steps and the consumer of each message on a worker
     * thread pool.
     * Messages are processed sequentially.
     * Use this when the consumer performs blocking operations.
     *
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking();

    /**
     * Returns a new {@link ChannelBinding} that runs the processing steps and the consumer of each message on a worker
     * thread pool, processing up to {@code concurrency} messages in parallel.
     * Use this when the consumer performs blocking operations.
     *
     * @param concurrency the maximum number of messages processed concurrently
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking(int concurrency);

    /**
     * Returns a new {@link ChannelBinding} that runs the processing steps and the consumer of each message on the given
     * executor.
     * Messages are processed sequentially.
     * Use this when the consumer performs blocking operations.
     *
     * @param executor the executor to run the consumer on
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking(Executor executor);

    /**
     * Returns a new {@link ChannelBinding} that runs the processing steps and the consumer of each message on the given
     * executor, processing up to {@code concurrency} messages in parallel.
     * Use this when the consumer performs blocking operations.
     *
     * @param executor the executor to run the consumer on
     * @param concurrency the maximum number of messages processed concurrently
     * @return a new blocking {@link ChannelBinding}
     */
    ChannelBinding<I, O> blocking(Executor executor, int concurrency);

    /**
     * Adds a transformation step, applied to the current output: the payload, or the result of the previous step.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the transformation function
     * @param <T> the result type of the step
     * @return a new {@link ChannelBinding} with the step added after the existing ones
     */
    <T> ChannelBinding<I, T> process(Function<O, T> processor);

    /**
     * Adds a transformation step with metadata access, applied to the current output: the payload, or the result of
     * the previous step.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the transformation function, receiving the current output and the message metadata
     * @param <T> the result type of the step
     * @return a new {@link ChannelBinding} with the step added after the existing ones
     */
    <T> ChannelBinding<I, T> process(BiFunction<O, Metadata, T> processor);

    /**
     * Adds an async transformation step, applied to the current output: the payload, or the result of the previous
     * step.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the async transformation function
     * @param <T> the result type of the step
     * @return a new {@link ChannelBinding} with the step added after the existing ones
     */
    <T> ChannelBinding<I, T> processAsync(Function<O, Uni<T>> processor);

    /**
     * Adds an async transformation step with metadata access, applied to the current output: the payload, or the
     * result of the previous step.
     * Use {@link #to(Flow.Subscriber)} to wire the processed stream to an outgoing channel.
     *
     * @param processor the async transformation function, receiving the current output and the message metadata
     * @param <T> the result type of the step
     * @return a new {@link ChannelBinding} with the step added after the existing ones
     */
    <T> ChannelBinding<I, T> processAsync(BiFunction<O, Metadata, Uni<T>> processor);

    /**
     * Wires the stream to an outgoing subscriber.
     * If processing steps were added, each message is transformed by them before forwarding.
     * Otherwise, messages are forwarded as-is.
     * The outgoing message preserves the incoming message's metadata and ack/nack chain.
     *
     * @param subscriber the outgoing channel subscriber,
     *        typically obtained from {@link ChannelFactory#outgoing(OutgoingConnectorConfiguration)}
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable to(Flow.Subscriber<? extends Message<?>> subscriber);

    /**
     * Creates an outbound channel from the given connector configuration and wires this binding to it.
     * Combines {@link ChannelFactory#outgoing(OutgoingConnectorConfiguration)} and
     * {@link #to(Flow.Subscriber)} in one call.
     *
     * @param config the outgoing connector configuration
     * @return a {@link Cancellable} to cancel the subscription
     */
    Cancellable to(OutgoingConnectorConfiguration config);
}
