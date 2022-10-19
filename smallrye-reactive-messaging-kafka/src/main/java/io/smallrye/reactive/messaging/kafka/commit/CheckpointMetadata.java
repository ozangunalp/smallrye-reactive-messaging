package io.smallrye.reactive.messaging.kafka.commit;

import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Checkpoint metadata type for injecting state checkpointing interactions into received messages.
 * This allows accessing the current processing state restored from the state store, and produce the next state.
 * The next state can be saved locally or persisted into the external store.
 *
 * <p>
 * A sample processing method with checkpointing would be:
 *
 * <pre>
 * &#64;Incoming("in")
 * public CompletionStage&lt;Void&gt; process(Message&lt;String&gt; msg) {
 *     CheckpointMetadata&lt;Integer&gt; checkpoint = CheckpointMetadata.fromMessage(msg);
 *     if (checkpoint != null) {
 *         checkpoint.transform(0, current -> current + msg.getPayload());
 *     }
 *     return CompletableFuture.completed(null);
 * }
 * </pre>
 *
 * @param <T> type of the processing state
 */
public interface CheckpointMetadata<T> {

    @SuppressWarnings("unchecked")
    static <S> CheckpointMetadata<S> fromMessage(Message<?> message) {
        return message.getMetadata(CheckpointMetadata.class).orElse(null);
    }

    TopicPartition getTopicPartition();

    long getRecordOffset();

    boolean isPersistOnAck();

    Optional<ProcessingState<T>> getCurrent();

    Optional<ProcessingState<T>> getNext();

    T setNext(T state, long offset, boolean persistOnAck);

    T setNext(T state, long offset);

    T setNext(T state);

    T setNext(T state, boolean persistOnAck);

    T transform(T initialState, Function<T, T> transformation, boolean persistOnAck);

    T transform(T initialState, Function<T, T> transformation);
}
