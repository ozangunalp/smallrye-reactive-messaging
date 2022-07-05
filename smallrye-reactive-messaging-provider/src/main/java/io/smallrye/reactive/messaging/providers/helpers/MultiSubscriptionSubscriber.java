package io.smallrye.reactive.messaging.providers.helpers;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;

import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;

public abstract class MultiSubscriptionSubscriber<I, O> implements Subscription, MultiSubscriber<O>, ContextSupport {

    final MultiSubscriber<? super O> actual;

    protected boolean unbounded;
    /**
     * The current subscription which may null if no Subscriptions have been set.
     */
    Subscription subscription;
    /**
     * The current outstanding request amount.
     */
    long requested;
    volatile Subscription missedSubscription;
    volatile long missedRequested;
    volatile long missedProduced;
    volatile int wip;
    volatile boolean cancelled;

    public MultiSubscriptionSubscriber(MultiSubscriber<? super O> actual) {
        this.actual = actual;
    }

    @Override
    public void cancel() {
        if (!cancelled) {
            cancelled = true;

            drain();
        }
    }

    public final boolean isUnbounded() {
        return unbounded;
    }

    final boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void onCompletion() {
        actual.onComplete();
    }

    @Override
    public void onFailure(Throwable t) {
        actual.onError(t);
    }

    @Override
    public void onSubscribe(Subscription s) {
        set(s);
    }

    public final void produced(long n) {
        if (unbounded) {
            return;
        }
        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
            long r = requested;

            if (r != Long.MAX_VALUE) {
                long u = r - n;
                if (u < 0L) {
                    // report more produced
                    u = 0;
                }
                requested = u;
            } else {
                unbounded = true;
            }

            if (WIP.decrementAndGet(this) == 0) {
                return;
            }

            drainLoop();

            return;
        }

        addCap(MISSED_PRODUCED, this, n);

        drain();
    }

    final void producedOne() {
        if (unbounded) {
            return;
        }
        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
            long r = requested;

            if (r != Long.MAX_VALUE) {
                r--;
                if (r < 0L) {
                    // report more produced
                    r = 0;
                }
                requested = r;
            } else {
                unbounded = true;
            }

            if (WIP.decrementAndGet(this) == 0) {
                return;
            }

            drainLoop();

            return;
        }

        addCap(MISSED_PRODUCED, this, 1L);

        drain();
    }

    @Override
    public final void request(long n) {
        if (validate(n)) {
            if (unbounded) {
                return;
            }
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;

                if (r != Long.MAX_VALUE) {
                    r = addCap(r, n);
                    requested = r;
                    if (r == Long.MAX_VALUE) {
                        unbounded = true;
                    }
                }
                Subscription a = subscription;

                if (WIP.decrementAndGet(this) != 0) {
                    drainLoop();
                }

                if (a != null) {
                    a.request(n);
                }

                return;
            }

            addCap(MISSED_REQUESTED, this, n);

            drain();
        }
    }

    public final void set(Subscription s) {
        if (cancelled) {
            s.cancel();
            return;
        }

        Objects.requireNonNull(s);

        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
            Subscription a = subscription;

            if (a != null && shouldCancelCurrent()) {
                a.cancel();
            }

            subscription = s;

            long r = requested;

            if (WIP.decrementAndGet(this) != 0) {
                drainLoop();
            }

            if (r != 0L) {
                s.request(r);
            }

            return;
        }

        Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
        if (a != null && shouldCancelCurrent()) {
            a.cancel();
        }
        drain();
    }

    /**
     * When setting a new subscription via set(), should
     * the previous subscription be cancelled?
     *
     * @return true if cancellation is needed
     */
    protected boolean shouldCancelCurrent() {
        return false;
    }

    final void drain() {
        if (WIP.getAndIncrement(this) != 0) {
            return;
        }
        drainLoop();
    }

    final void drainLoop() {
        int missed = 1;

        long requestAmount = 0L;
        long alreadyInRequestAmount = 0L;
        Subscription requestTarget = null;

        for (;;) {

            Subscription ms = missedSubscription;

            if (ms != null) {
                ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
            }

            long mr = missedRequested;
            if (mr != 0L) {
                mr = MISSED_REQUESTED.getAndSet(this, 0L);
            }

            long mp = missedProduced;
            if (mp != 0L) {
                mp = MISSED_PRODUCED.getAndSet(this, 0L);
            }

            Subscription a = subscription;

            if (cancelled) {
                if (a != null) {
                    a.cancel();
                    subscription = null;
                }
                if (ms != null) {
                    ms.cancel();
                }
            } else {
                long r = requested;
                if (r != Long.MAX_VALUE) {
                    long u = addCap(r, mr);

                    if (u != Long.MAX_VALUE) {
                        long v = u - mp;
                        if (v < 0L) {
                            // report more produced
                            v = 0;
                        }
                        r = v;
                    } else {
                        r = u;
                    }
                    requested = r;
                }

                if (ms != null) {
                    if (a != null && shouldCancelCurrent()) {
                        a.cancel();
                    }
                    subscription = ms;
                    if (r != 0L) {
                        requestAmount = addCap(requestAmount, r - alreadyInRequestAmount);
                        requestTarget = ms;
                    }
                } else if (mr != 0L && a != null) {
                    requestAmount = addCap(requestAmount, mr);
                    alreadyInRequestAmount += mr;
                    requestTarget = a;
                }
            }

            missed = WIP.addAndGet(this, -missed);
            if (missed == 0) {
                if (requestAmount != 0L) {
                    System.out.println("replenish " + requestAmount);
                    requestTarget.request(requestAmount);
                }
                return;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<MultiSubscriptionSubscriber, Subscription> MISSED_SUBSCRIPTION = AtomicReferenceFieldUpdater
            .newUpdater(MultiSubscriptionSubscriber.class,
                    Subscription.class,
                    "missedSubscription");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_REQUESTED = AtomicLongFieldUpdater
            .newUpdater(MultiSubscriptionSubscriber.class, "missedRequested");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_PRODUCED = AtomicLongFieldUpdater
            .newUpdater(MultiSubscriptionSubscriber.class, "missedProduced");
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<MultiSubscriptionSubscriber> WIP = AtomicIntegerFieldUpdater
            .newUpdater(MultiSubscriptionSubscriber.class, "wip");

    public static long addCap(long a, long b) {
        long res = a + b;
        if (res < 0L) {
            return Long.MAX_VALUE;
        }
        return res;
    }

    public static <T> long addCap(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
        long r, u;
        for (;;) {
            r = updater.get(instance);
            if (r == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            u = addCap(r, toAdd);
            if (updater.compareAndSet(instance, r, u)) {
                return r;
            }
        }
    }

    /**
     * Check Subscription current state and cancel new Subscription if current is set,
     * or return true if ready to subscribe.
     *
     * @param current current Subscription, expected to be null
     * @param next new Subscription
     * @return true if Subscription can be used
     */
    public static boolean validate(Subscription current, Subscription next) {
        Objects.requireNonNull(next, "Subscription cannot be null");
        if (current != null) {
            next.cancel();
            //reportSubscriptionSet();
            return false;
        }

        return true;
    }

    /**
     * Evaluate if a request is strictly positive otherwise false
     *
     * @param n the request value
     * @return true if valid
     */
    public static boolean validate(long n) {
        if (n <= 0) {
            return false;
        }
        return true;
    }
}
