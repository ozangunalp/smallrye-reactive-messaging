package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Default implementation for {@link CheckpointMetadata}
 *
 * @param <T> type of the processing state
 */
public class DefaultCheckpointMetadata<T> implements CheckpointMetadata<T> {

    private final TopicPartition topicPartition;
    private final long recordOffset;
    private final KafkaCheckpointCommit.CheckpointState checkpointState;
    private volatile ProcessingState<T> next;
    private volatile boolean persistOnAck;

    @SuppressWarnings("unchecked")
    public static <S> DefaultCheckpointMetadata<S> fromMessage(Message<?> message) {
        return (DefaultCheckpointMetadata<S>) message.getMetadata().get(DefaultCheckpointMetadata.class).orElse(null);
    }

    public DefaultCheckpointMetadata(TopicPartition topicPartition, long recordOffset,
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
