package io.smallrye.reactive.messaging.providers.extension;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.TableView;

public class DefaultTableView<K, V> extends AbstractMulti<Tuple2<K, V>> implements TableView<K, V> {

    /**
     * The upstream {@link Multi}.
     */
    protected final Multi<? extends Tuple2<K, V>> upstream;

    // High memory consumption
    private final Map<K, V> data;

    private static final Object NULL_KEY = new Object() {

        @Override
        public String toString() {
            return super.toString() + "NULL_KEY";
        }
    };
    private final boolean emitOnChange;

    public DefaultTableView(Multi<? extends Message<V>> upstream, Function<Message<V>, Tuple2<K, V>> tupleExtractor) {
        this(upstream, tupleExtractor, false);
    }

    public DefaultTableView(Multi<? extends Message<V>> upstream,
            Function<Message<V>, Tuple2<K, V>> tupleExtractor,
            boolean emitOnChange) {
        this(ParameterValidation.nonNull(upstream, "upstream")
                .onItem().transformToUniAndConcatenate(m -> Uni.createFrom().completionStage(m.ack()).map(x -> m))
                .onItem().transform(tupleExtractor),
                emitOnChange,
                true);
    }

    public DefaultTableView(Multi<? extends Tuple2<K, V>> upstream, boolean emitOnChange, boolean subscribeOnCreation) {
        this.data = new ConcurrentHashMap<>();
        this.upstream = ParameterValidation.nonNull(upstream, "upstream")
                .withContext((multi, context) -> {
                    context.put("table_data", this.data);
                    return multi;
                })
                .attachContext()
                .onItem().transformToUniAndConcatenate(t -> {
                    Tuple2<K, V> tuple = t.get();
                    Map<K, V> tableData = t.context().get("table_data");
                    K key = tuple.getItem1();
                    V value = tuple.getItem2();
                    V previous = updateEntry(tableData, key, value);
                    if (emitOnChange && Objects.equals(previous, value)) {
                        return Uni.createFrom().nullItem();
                    }
                    return Uni.createFrom().item(tuple);
                })
                .broadcast().toAllSubscribers();
        this.emitOnChange = emitOnChange;
        if (subscribeOnCreation) {
            this.subscribe().with(t -> {
            });
        }
    }

    private boolean isNullKey(Object key) {
        return key == NULL_KEY;
    }

    private V updateEntry(Map<K, V> data, K key, V value) {
        if (key == null) {
            if (value == null) {
                return data.remove(NULL_KEY);
            } else {
                return data.put((K) NULL_KEY, value);
            }
        } else {
            if (value == null) {
                return data.remove(key);
            } else {
                return data.put(key, value);
            }
        }
    }

    @Override
    public void subscribe(MultiSubscriber<? super Tuple2<K, V>> downstream) {
        if (downstream == null) {
            throw new NullPointerException("Subscriber is `null`");
        }
        this.upstream.subscribe().withSubscriber(downstream);
    }

    @Override
    public Set<K> keys() {
        return this.data.keySet()
                .stream().map(k -> isNullKey(k) ? null : k)
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return this.data.values();
    }

    @Override
    public Map<K, V> fetch(Collection<K> keys) {
        return this.data.entrySet().stream()
                .filter(e -> isNullKey(e.getKey()) ? keys.contains(null) : keys.contains(e.getKey()))
                .collect(Collectors.toMap(e -> isNullKey(e.getKey()) ? null : e.getKey(), Map.Entry::getValue));
    }

    @Override
    public Map<K, V> fetchAll() {
        return this.data.entrySet().stream()
                .collect(Collectors.toMap(e -> isNullKey(e.getKey()) ? null : e.getKey(), Map.Entry::getValue));
    }

    public <T> TableView<K, T> map(BiFunction<K, V, T> mapper) {
        return new DefaultTableView<>(
                this.upstream.map(t -> Tuple2.of(t.getItem1(), mapper.apply(t.getItem1(), t.getItem2()))),
                emitOnChange, false);
    }

    public <T> TableView<T, V> mapKey(BiFunction<K, V, T> mapper) {
        return new DefaultTableView<>(
                this.upstream.map(t -> Tuple2.of(mapper.apply(t.getItem1(), t.getItem2()), t.getItem2())),
                emitOnChange, false);
    }

    public <T> TableView<K, T> chain(BiFunction<K, V, Uni<T>> mapper) {
        return new DefaultTableView<>(this.upstream
                .onItem().transformToUniAndConcatenate(t -> mapper.apply(t.getItem1(), t.getItem2())
                        .map(v -> Tuple2.of(t.getItem1(), v))),
                emitOnChange, false);
    }

    public <T> TableView<T, V> chainKey(BiFunction<K, V, Uni<T>> mapper) {
        return new DefaultTableView<>(this.upstream
                .onItem().transformToUniAndConcatenate(t -> mapper.apply(t.getItem1(), t.getItem2())
                        .map(k -> Tuple2.of(k, t.getItem2()))),
                emitOnChange, false);
    }

    public TableView<K, V> filter(BiPredicate<K, V> predicate) {
        return new DefaultTableView<>(this.upstream.filter(t -> predicate.test(t.getItem1(), t.getItem2())),
                emitOnChange, false);
    }

    public TableView<K, V> filterKey(Predicate<K> predicate) {
        return new DefaultTableView<>(this.upstream.filter(t -> predicate.test(t.getItem1())),
                emitOnChange, false);
    }

    public TableView<K, V> withEmitOnChange() {
        return new DefaultTableView<>(this.upstream, true, false);
    }

    public TableView<K, V> subscribeNow() {
        this.subscribe().with(t -> {
        });
        return this;
    }

    public TableView<K, V> filterKey(K keyToMatch) {
        return filterKey(k -> Objects.equals(k, keyToMatch));
    }

    @Override
    public Multi<Map<K, V>> toMapStream() {
        return this.onItem().scan(() -> new ConcurrentHashMap<>(this.data), (m, t) -> {
            m.put(t.getItem1(), t.getItem2());
            return m;
        });
    }

    @Override
    public V get(K key) {
        return this.data.get(key);
    }

}
