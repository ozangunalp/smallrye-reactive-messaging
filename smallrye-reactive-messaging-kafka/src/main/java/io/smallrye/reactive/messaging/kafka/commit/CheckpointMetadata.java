package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

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
public class CheckpointMetadata<T> {

    private final TopicPartition topicPartition;
    private final long recordOffset;
    private final KafkaCheckpointCommit.CheckpointState checkpointState;
    private ProcessingState<T> next;
    private boolean persistOnAck;

    @SuppressWarnings("unchecked")
    public static <S> ProcessingState<S> getNextState(Message<?> message) {
        return (ProcessingState<S>) message.getMetadata(CheckpointMetadata.class)
                .flatMap(CheckpointMetadata::getNext).orElse(null);
    }

    public static boolean isPersist(Message<?> message) {
        return message.getMetadata(CheckpointMetadata.class).map(CheckpointMetadata::isPersistOnAck).orElse(false);
    }

    @SuppressWarnings("unchecked")
    public static <S> CheckpointMetadata<S> fromMessage(Message<?> message) {
        return (CheckpointMetadata<S>) message.getMetadata(CheckpointMetadata.class).orElse(null);
    }

    public CheckpointMetadata(TopicPartition topicPartition, long recordOffset,
            KafkaCheckpointCommit.CheckpointState checkpointState) {
        this.topicPartition = topicPartition;
        this.recordOffset = recordOffset;
        this.checkpointState = checkpointState;
    }

    KafkaCheckpointCommit.CheckpointState getCheckpointState() {
        return checkpointState;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public long getRecordOffset() {
        return recordOffset;
    }

    public boolean isPersistOnAck() {
        return persistOnAck;
    }

    public Optional<ProcessingState<T>> getCurrent() {
        return Optional.ofNullable((ProcessingState<T>) checkpointState.getProcessingState());
    }

    public Optional<ProcessingState<T>> getNext() {
        return Optional.ofNullable(next);
    }

    public T setNext(T state, long offset, boolean persistOnAck) {
        this.next = new ProcessingState<>(state, offset);
        this.persistOnAck = persistOnAck;
        return this.next.getState();
    }

    public T setNext(T state, long offset) {
        return setNext(state, offset, false);
    }

    public T setNext(T state) {
        return setNext(state, false);
    }

    public T setNext(T state, boolean persistOnAck) {
        return setNext(state, getRecordOffset() + 1, persistOnAck);
    }

    public T transform(T initialState, Function<T, T> transformation, boolean persistOnAck) {
        ProcessingState<T> processingState = ProcessingState
                .getOrDefault((ProcessingState<T>) checkpointState.getProcessingState(), initialState);
        if (recordOffset >= processingState.getOffset()) {
            return setNext(transformation.apply(processingState.getState()), persistOnAck);
        } else {
            log.debugf("Skipping transformation on %s:%d, latest processing state offset %d",
                    topicPartition, recordOffset, processingState.getOffset());
            return processingState.getState();
        }
    }

    public T transform(T initialState, Function<T, T> transformation) {
        return transform(initialState, transformation, false);
    }
}
