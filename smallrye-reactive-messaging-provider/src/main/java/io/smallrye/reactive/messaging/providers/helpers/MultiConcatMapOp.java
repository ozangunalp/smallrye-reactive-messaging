package io.smallrye.reactive.messaging.providers.helpers;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.AbstractMultiOperator;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public class MultiConcatMapOp<I, O> extends AbstractMultiOperator<I, O> {

    private final Function<? super I, ? extends Publisher<? extends O>> mapper;

    private final boolean postponeFailurePropagation;

    public MultiConcatMapOp(Multi<? extends I> upstream,
            Function<? super I, ? extends Publisher<? extends O>> mapper,
            boolean postponeFailurePropagation) {
        super(upstream);
        this.mapper = mapper;
        this.postponeFailurePropagation = postponeFailurePropagation;
    }

    @Override
    public void subscribe(MultiSubscriber<? super O> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("The subscriber must not be `null`");
        }
        ConcatMapMainSubscriber<I, O> sub = new ConcatMapMainSubscriber<>(subscriber,
                mapper,
                postponeFailurePropagation);

        upstream.subscribe(Infrastructure.onMultiSubscription(upstream, sub));
    }

    public static final class ConcatMapMainSubscriber<I, O> implements MultiSubscriber<I>, Subscription, ContextSupport {

        enum State {
            INITIAL,
            /**
             * Requested from {@link #upstream}, waiting for {@link #onNext(Object)}
             */
            REQUESTED,
            /**
             * {@link #onNext(Object)} received, listening on {@link #inner}
             */
            ACTIVE,
            /**
             * Received outer {@link #onComplete()}, waiting for {@link #inner} to complete
             */
            LAST_ACTIVE,
            /**
             * Terminated either successfully or after an error
             */
            TERMINATED,
            CANCELLED,
        }

        volatile State state;

        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ConcatMapMainSubscriber, State> STATE = AtomicReferenceFieldUpdater.newUpdater(
                ConcatMapMainSubscriber.class,
                State.class,
                "state");

        volatile Throwable error;

        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ConcatMapMainSubscriber, Throwable> ERROR = AtomicReferenceFieldUpdater
                .newUpdater(
                        ConcatMapMainSubscriber.class,
                        Throwable.class,
                        "error");

        final MultiSubscriber<? super O> actual;

        final ConcatMapInner<O> inner;

        final Function<? super I, ? extends Publisher<? extends O>> mapper;
        private boolean postponeFailurePropagation;

        Subscription upstream;

        private final AtomicLong requested = new AtomicLong();

        ConcatMapMainSubscriber(
                MultiSubscriber<? super O> actual,
                Function<? super I, ? extends Publisher<? extends O>> mapper,
                boolean postponeFailurePropagation) {
            this.actual = actual;
            this.mapper = mapper;
            this.postponeFailurePropagation = postponeFailurePropagation;
            this.inner = new ConcatMapInner<>(this);
            STATE.lazySet(this, State.INITIAL);
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.upstream = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onItem(I item) {
            if (!STATE.compareAndSet(this, State.REQUESTED, State.ACTIVE)) {
                switch (state) {
                    case CANCELLED:
                    case TERMINATED:
                        break;
                }
                return;
            }

            try {
                Publisher<? extends O> p = mapper.apply(item);
                Objects.requireNonNull(p, "The mapper returned a null Publisher");

                p.subscribe(inner);
            } catch (Throwable e) {
                if (!maybeOnError(e, upstream)) {
                    innerComplete();
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (!maybeOnError(t, inner)) {
                onCompletion();
            }
        }

        @Override
        public void onCompletion() {
            for (State previousState = this.state;; previousState = this.state) {
                switch (previousState) {
                    case INITIAL:
                    case REQUESTED:
                        if (!STATE.compareAndSet(this, previousState, State.TERMINATED)) {
                            continue;
                        }

                        Throwable ex = error;
                        if (ex != null) {
                            actual.onFailure(ex);
                            return;
                        }
                        actual.onCompletion();
                        return;
                    case ACTIVE:
                        if (!STATE.compareAndSet(this, previousState, State.LAST_ACTIVE)) {
                            continue;
                        }
                        return;
                    default:
                        return;
                }
            }
        }

        public synchronized void tryEmit(O value) {
            switch (state) {
                case ACTIVE:
                case LAST_ACTIVE:
                    actual.onItem(value);
                    break;
                default:
                    break;
            }
        }

        public void innerComplete() {
            for (State previousState = this.state;; previousState = this.state) {
                switch (previousState) {
                    case ACTIVE:
                        if (!STATE.compareAndSet(this, previousState, State.REQUESTED)) {
                            continue;
                        }
                        // inner completed but there are outstanding requests from inner,
                        // or the inner is completed without starting producing items
                        // request new items from upstream
                        if (inner.requested != 0 || inner.currentProduced.get() == 0L) {
                            upstream.request(1);
                        }
                        return;
                    case LAST_ACTIVE:
                        if (!STATE.compareAndSet(this, previousState, State.TERMINATED)) {
                            continue;
                        }

                        Throwable ex = error;
                        if (ex != null) {
                            actual.onFailure(ex);
                            return;
                        }
                        actual.onCompletion();
                        return;
                    default:
                        return;
                }
            }
        }

        public void innerError(Throwable e) {
            if (!maybeOnError(e, upstream)) {
                innerComplete();
            }
        }

        private boolean maybeOnError(Throwable e, Subscription subscriptionToCancel) {
            if (e == null) {
                return false;
            }

            if (!ERROR.compareAndSet(this, null, e)) {
            }

            for (State previousState = this.state;; previousState = this.state) {
                switch (previousState) {
                    case CANCELLED:
                    case TERMINATED:
                        return true;
                    default:
                        if (!STATE.compareAndSet(this, previousState, State.TERMINATED)) {
                            continue;
                        }
                        subscriptionToCancel.cancel();
                        synchronized (this) {
                            actual.onFailure(error);
                        }
                        return true;
                }
            }
        }

        @Override
        public void request(long n) {
            if (STATE.compareAndSet(this, State.INITIAL, State.REQUESTED)) {
                //                System.out.println("initial upstream request");
                upstream.request(1);
                // No outstanding requests from inner, forward the request upstream
            } else if (STATE.get(this) == State.REQUESTED && inner.requested == 0) {
                //                System.out.println("forward upstream request");
                upstream.request(1);
            }
            inner.request(n);
        }

        @Override
        public void cancel() {
            switch (STATE.getAndSet(this, State.CANCELLED)) {
                case CANCELLED:
                    break;
                case TERMINATED:
                    inner.cancel();
                    break;
                default:
                    inner.cancel();
                    upstream.cancel();
            }
        }
    }

    static final class ConcatMapInner<O> extends MultiSubscriptionSubscriber<O, O> {
        private final ConcatMapMainSubscriber<?, O> parent;
        long produced;
        public AtomicLong currentProduced = new AtomicLong();

        ConcatMapInner(ConcatMapMainSubscriber<?, O> parent) {
            super(new MultiSubscriber<O>() {
                @Override
                public void onItem(O item) {
                    throw new RuntimeException();
                }

                @Override
                public void onFailure(Throwable failure) {
                    throw new RuntimeException();
                }

                @Override
                public void onCompletion() {
                    throw new RuntimeException();
                }

                @Override
                public void onSubscribe(Subscription subscription) {
                    throw new RuntimeException();
                }
            });
            this.parent = parent;
        }

        @Override
        public void onSubscribe(Subscription s) {
            currentProduced.set(0L);
            super.onSubscribe(s);
        }

        @Override
        public void onItem(O item) {
            produced++;
            currentProduced.incrementAndGet();
            parent.tryEmit(item);
        }

        @Override
        public void onFailure(Throwable failure) {
            long p = produced;

            if (p != 0L) {
                produced = 0L;
                produced(p);
            }

            parent.innerError(failure);
        }

        @Override
        public void onCompletion() {
            long p = produced;

            if (p != 0L) {
                produced = 0L;
                produced(p);
            }

            parent.innerComplete();
        }

        @Override
        public Context context() {
            return parent.context();
        }
    }
}
