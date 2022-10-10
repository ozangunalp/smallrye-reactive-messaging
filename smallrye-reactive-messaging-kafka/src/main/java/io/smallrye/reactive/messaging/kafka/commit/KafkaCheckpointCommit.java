package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.commit.CheckpointMetadata.isPersist;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.mutiny.core.Vertx;

/**
 * Commit handler for checkpointing processing state persisted in a state store
 * <p>
 * Instead of committing topic-partition offsets back to Kafka, checkpointing commit handlers persist and restore offsets on an
 * external store.
 * It associates a {@link ProcessingState} with a topic-partition offset, and lets the processing resume from the checkpointed
 * state.
 * <p>
 * This abstract implementation holds a local map of {@link ProcessingState} per topic-partition,
 * and ensures it is accessed on the captured Vert.x context.
 * <p>
 */
@Experimental("Experimental API")
public class KafkaCheckpointCommit extends ContextHolder implements KafkaCommitHandler {

    public static final String CHECKPOINT_COMMIT_NAME = "checkpoint";

    private final Map<TopicPartition, CheckpointState> checkpointStateMap = new ConcurrentHashMap<>();

    private volatile long timerId = -1;

    private final int autoCommitInterval;

    private final KafkaConsumer<?, ?> consumer;
    private final StateStore stateStore;

    private final BiConsumer<Throwable, Boolean> reportFailure;
    private final String consumerId;
    private final int nonpersistedStateMaxAge;

    public KafkaCheckpointCommit(Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            StateStore stateStore,
            BiConsumer<Throwable, Boolean> reportFailure,
            int autoCommitInterval,
            int nonpersistedStateMaxAge,
            int defaultTimeout) {
        super(vertx, defaultTimeout);
        this.consumer = consumer;
        this.consumerId = (String) consumer.configuration().get(ConsumerConfig.CLIENT_ID_CONFIG);
        this.stateStore = stateStore;
        this.reportFailure = reportFailure;
        this.autoCommitInterval = autoCommitInterval;
        this.nonpersistedStateMaxAge = nonpersistedStateMaxAge;
        if (nonpersistedStateMaxAge <= 0) {
            log.disableCheckpointCommitStrategyHealthCheck(consumerId);
        } else {
            log.setCheckpointCommitStrategyNonpersistedStateMaxAge(consumerId, nonpersistedStateMaxAge);
        }
    }

    @ApplicationScoped
    @Identifier(CHECKPOINT_COMMIT_NAME)
    public static class Factory implements KafkaCommitHandler.Factory {

        Instance<StateStore.Factory> stateStoreFactory;

        @Inject
        public Factory(@Any Instance<StateStore.Factory> stateStoreFactory) {
            this.stateStoreFactory = stateStoreFactory;
        }

        @Override
        public KafkaCommitHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure) {
            String groupId = (String) consumer.configuration().get(ConsumerConfig.GROUP_ID_CONFIG);
            int defaultTimeout = config.config()
                    .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                    .orElse(60000);
            int autoCommitInterval = config.config()
                    .getOptionalValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.class)
                    .orElse(5000);
            log.settingCommitInterval(groupId, autoCommitInterval);
            String stateStoreIdentifier = config.getCheckpointStateStore().orElseGet(() -> {
                log.checkpointDefaultStateStore();
                return "file";
            });
            StateStore.Factory factory = stateStoreFactory.select(Identifier.Literal.of(stateStoreIdentifier)).get();
            StateStore stateStore = factory.create(config, vertx);
            return new KafkaCheckpointCommit(vertx, consumer, stateStore, reportFailure, autoCommitInterval,
                    config.getCheckpointUnpersistedStateMaxAgeMs(),
                    defaultTimeout);
        }
    }

    /**
     * Cancel the existing timer.
     * Must be called from the event loop.
     */
    private void stopFlushAndCheckHealthTimer() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
            timerId = -1;
        }
    }

    /**
     * Schedule the next commit.
     * Must be called form the event loop.
     */
    private void startFlushAndCheckHealthTimer() {
        if (!checkpointStateMap.isEmpty()) {
            timerId = vertx.setTimer(autoCommitInterval, x -> runOnContext(this::flushAndCheckHealth));
        }
    }

    private void flushAndCheckHealth() {
        this.persistProcessingState(checkpointStateMap)
                .onItemOrFailure().invoke(() -> {
                    this.startFlushAndCheckHealthTimer();
                    checkHealth();
                }).subscribe().with(unused -> {
                });
    }

    private void checkHealth() {
        if (this.nonpersistedStateMaxAge > 0) {
            for (Map.Entry<TopicPartition, CheckpointState> state : checkpointStateMap.entrySet()) {
                TopicPartition tp = state.getKey();
                CheckpointState checkpointState = state.getValue();
                long elapsed = checkpointState.millisSinceLastPersistedOffset();
                boolean waitedTooLong = elapsed > nonpersistedStateMaxAge;
                if (waitedTooLong) {
                    LastStateStoredTooLongAgoException exception = new LastStateStoredTooLongAgoException(tp,
                            elapsed / 1000,
                            checkpointState.processingState.getOffset(),
                            checkpointState.persistedAt.getOffset());
                    log.warnf(exception, exception.getMessage());
                    this.reportFailure.accept(exception, true);
                }
            }
        }
    }

    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        return Uni.createFrom().item(record)
                .emitOn(this::runOnContext) // state map is accessed on the captured context
                .onItem().transform(r -> {
                    TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
                    CheckpointState state = checkpointStateMap.get(tp);
                    if (state != null) {
                        state.receivedRecord();
                    }
                    r.injectMetadata(
                            new CheckpointMetadata<>(tp, record.getOffset(), () -> this.getCurrentProcessingState(tp)));
                    return r;
                }).onItem().invoke(() -> {
                    if (timerId < 0) {
                        startFlushAndCheckHealthTimer();
                    }
                });
    }

    private ProcessingState<?> getCurrentProcessingState(TopicPartition tp) {
        CheckpointState checkpointState = checkpointStateMap.get(tp);
        return checkpointState != null ? checkpointState.getProcessingState() : null;
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());
        if (checkpointStateMap.containsKey(tp)) {
            ProcessingState<?> newState = CheckpointMetadata.getNextState(record);
            boolean persist = isPersist(record);
            if (!ProcessingState.isEmptyOrNull(newState)) {
                return Uni.createFrom().item(newState)
                        .emitOn(this::runOnContext) // state map is accessed on the captured context
                        .onItem().transform(state -> checkpointStateMap.compute(tp,
                                (t, s) -> (s != null) ? s.withNewState(state) : new CheckpointState(state)))
                        .onItem().invoke(s -> s.processedRecord())
                        .chain(s -> persist ? this.persistProcessingState(Map.of(tp, s))
                                : Uni.createFrom().voidItem())
                        .emitOn(record::runOnMessageContext)
                        .replaceWithVoid();
            } else {
                return Uni.createFrom().voidItem()
                        .emitOn(this::runOnContext)
                        .onItem().invoke(() -> checkpointStateMap.get(tp).processedRecord())
                        .emitOn(record::runOnMessageContext);
            }
        } else {
            log.acknowledgementFromRevokedTopicPartition(record.getOffset(), tp, consumerId, checkpointStateMap.keySet());
        }
        return Uni.createFrom().voidItem();
    }

    @Override
    public void terminate(boolean graceful) {
        // TODO wait for processing when graceful
        removeFromState(checkpointStateMap.keySet())
                .onItem().invoke(this::stopFlushAndCheckHealthTimer)
                .chain(this::persistProcessingState)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
        stateStore.close();
    }

    private Uni<Map<TopicPartition, CheckpointState>> initState(Collection<TopicPartition> partitions) {
        return Uni.createFrom().item(partitions)
                .onItem().transform(p -> {
                    p.forEach(tp -> checkpointStateMap.putIfAbsent(tp, new CheckpointState()));
                    return checkpointStateMap;
                }).runSubscriptionOn(this::runOnContext); // state map is accessed on the captured context
    }

    @Override
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        Map<TopicPartition, ? extends ProcessingState<?>> fetchedStates = initState(partitions)
                .onItem().invoke(this::stopFlushAndCheckHealthTimer)
                .chain(map -> stateStore.fetchProcessingState(partitions)
                        .onItem().invoke(fetched ->
                                log.checkpointPartitionsAssigned(consumerId, partitions, fetched.toString()))
                        .onFailure().invoke(f -> log.failedCheckpointPartitionsAssigned(consumerId, partitions, f))
                        .emitOn(this::runOnContext) // state map is accessed on the captured context
                        .onItem().invoke(fetched -> {
                            for (Map.Entry<TopicPartition, ProcessingState<?>> entry : fetched.entrySet()) {
                                ProcessingState<?> state = entry.getValue();
                                if (!ProcessingState.isEmptyOrNull(state)) {
                                    map.compute(entry.getKey(),
                                            (tp, s) -> (s != null) ? s.withNewState(state) : new CheckpointState(state));
                                }
                            }
                        }))
                .emitOn(this::runOnContext)
                .onItem().invoke(this::startFlushAndCheckHealthTimer)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
        Consumer<?, ?> kafkaConsumer = consumer.unwrap();
        for (Map.Entry<TopicPartition, ? extends ProcessingState<?>> entry : fetchedStates.entrySet()) {
            ProcessingState<?> state = entry.getValue();
            kafkaConsumer.seek(entry.getKey(), state != null ? state.getOffset() : 0L);
        }
    }

    private Uni<Map<TopicPartition, CheckpointState>> removeFromState(Collection<TopicPartition> partitions) {
        return Uni.createFrom().item(partitions)
                .onItem().transform(tp -> {
                    Map<TopicPartition, CheckpointState> toRemove = new HashMap<>();
                    tp.forEach(p -> {
                        CheckpointState removed = checkpointStateMap.remove(p);
                        if (removed != null) {
                            toRemove.put(p, removed);
                        }
                    });
                    return toRemove;
                }).runSubscriptionOn(this::runOnContext); // state map is accessed on the captured context
    }

    @Override
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        removeFromState(partitions)
                .onItem()
                .invoke(map -> log.checkpointPartitionsRevoked(consumerId, partitions, map.toString()))
                .onItem().invoke(this::stopFlushAndCheckHealthTimer)
                .chain(this::persistProcessingState)
                .emitOn(this::runOnContext)
                .onItem().invoke(this::startFlushAndCheckHealthTimer)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
    }

    Uni<Void> persistProcessingState(Map<TopicPartition, CheckpointState> stateMap) {
        Map<TopicPartition, ProcessingState<?>> map = new HashMap<>();
        for (Map.Entry<TopicPartition, CheckpointState> entry : stateMap.entrySet()) {
            CheckpointState checkpointState = entry.getValue();
            if (checkpointState.hasNonpersistedOffset()) {
                map.put(entry.getKey(), checkpointState.getProcessingState());
            }
        }
        if (map.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return stateStore.persistProcessingState(map)
                .onItem().invoke(() -> map.forEach((tp, state) -> checkpointStateMap.computeIfPresent(tp,
                        (t, s) -> s.withPersistedAt(OffsetPersistedAt.persisted(state.getOffset())))))
                .onItem().invoke(() -> log.checkpointPersistedState(consumerId, checkpointStateMap.toString()))
                .onFailure().invoke(t -> log.checkpointFailedPersistingState(consumerId, checkpointStateMap.toString(), t));
    }

    private static class OffsetPersistedAt {
        private final long offset;
        private final long persistedAt;

        public static OffsetPersistedAt NOT_PERSISTED = new OffsetPersistedAt(-1, -1);

        public static OffsetPersistedAt persisted(long offset) {
            return new OffsetPersistedAt(offset, System.currentTimeMillis());
        }

        private OffsetPersistedAt(long offset, long persistedAt) {
            this.offset = offset;
            this.persistedAt = persistedAt;
        }

        public boolean notPersisted() {
            return NOT_PERSISTED.equals(this);
        }

        public long getOffset() {
            return offset;
        }

        public long getPersistedAt() {
            return persistedAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            OffsetPersistedAt that = (OffsetPersistedAt) o;
            return offset == that.offset && persistedAt == that.persistedAt;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, persistedAt);
        }

        @Override
        public String toString() {
            return "OffsetPersistedAt{" +
                    "offset=" + offset +
                    ", persistedAt=" + persistedAt +
                    '}';
        }
    }

    public static class LastStateStoredTooLongAgoException extends NoStackTraceThrowable {

        public LastStateStoredTooLongAgoException(TopicPartition topic, long time, long currentStateOffset,
                long lastStoredOffset) {
            super(String.format("Latest processing state for topic-partition `%s` persisted %d seconds ago. " +
                    "At the moment latest registered local processing state is for offset %d. " +
                    "The last offset for which a state is successfully persisted was %d.",
                    topic, time, currentStateOffset, lastStoredOffset));
        }
    }

    static class CheckpointState {
        private final long createdTimestamp;
        private final AtomicInteger unprocessedRecords;
        private ProcessingState<?> processingState;
        private OffsetPersistedAt persistedAt;

        private CheckpointState(ProcessingState<?> processingState, OffsetPersistedAt persistedAt,
                AtomicInteger unprocessedRecords) {
            this.createdTimestamp = System.currentTimeMillis();
            this.processingState = processingState;
            this.persistedAt = persistedAt;
            this.unprocessedRecords = unprocessedRecords;
        }

        public CheckpointState() {
            this(ProcessingState.EMPTY_STATE);
        }

        public CheckpointState(ProcessingState<?> processingState) {
            this(processingState, OffsetPersistedAt.NOT_PERSISTED, new AtomicInteger(0));
        }

        public CheckpointState withPersistedAt(OffsetPersistedAt offsetPersistedAt) {
            this.persistedAt = offsetPersistedAt;
            return this;
        }

        public CheckpointState withNewState(ProcessingState<?> state) {
            this.processingState = state;
            return this;
        }

        public ProcessingState<?> getProcessingState() {
            return processingState;
        }

        public OffsetPersistedAt getPersistedAt() {
            return persistedAt;
        }

        public AtomicInteger getUnprocessedRecords() {
            return unprocessedRecords;
        }

        public void processedRecord() {
            unprocessedRecords.decrementAndGet();
        }

        public void receivedRecord() {
            unprocessedRecords.incrementAndGet();
        }

        public long millisSinceLastPersistedOffset() {
            // state never persisted, count the
            if (persistedAt.notPersisted()) {
                return System.currentTimeMillis() - createdTimestamp;
            } else if (hasNonpersistedOffset()) {
                return System.currentTimeMillis() - persistedAt.getPersistedAt();
            } else {
                return -1;
            }
        }

        public boolean hasNonpersistedOffset() {
            return !ProcessingState.isEmptyOrNull(processingState) && processingState.getOffset() > persistedAt.getOffset();
        }

        @Override
        public String toString() {
            return "CheckpointState{" +
                    "processingState=" + processingState +
                    ", persistedAt=" + persistedAt +
                    ", unprocessedRecords=" + unprocessedRecords +
                    '}';
        }
    }

}
