package io.smallrye.reactive.messaging.providers.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.TimeoutException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.connector.ConnectorLifecycle;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.extension.CompositePausableChannel;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;

@ApplicationScoped
public class ChannelLifecycleManagerImpl implements ChannelLifecycleManager {

    private static final Logger log = Logger.getLogger(ChannelLifecycleManagerImpl.class);

    /**
     * Global property overriding the timeout of each shutdown phase, in milliseconds.
     */
    public static final String PHASE_TIMEOUT_PROPERTY = "smallrye.messaging.graceful-shutdown.phase-timeout";

    /**
     * Drain timeout assumed when no channel is pausable, the default of {@code graceful-shutdown.drain-timeout}.
     */
    static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Added to the longest drain timeout, for the connectors' prepare-close and close calls.
     */
    static final Duration PHASE_TIMEOUT_MARGIN = Duration.ofSeconds(20);

    @Inject
    @Any
    ChannelRegistry registry;

    @Inject
    ConnectorFactories connectorFactories;

    @Inject
    Instance<Config> configInstance;

    private final AtomicBoolean prepareCloseIncomingDone = new AtomicBoolean();
    private final AtomicBoolean prepareCloseOutgoingDone = new AtomicBoolean();
    private final AtomicBoolean closeDone = new AtomicBoolean();

    // --- Prepare close incoming channels ---

    @Override
    public void prepareCloseIncoming() {
        if (!prepareCloseIncomingDone.compareAndSet(false, true)) {
            return;
        }

        try {
            List<Uni<Void>> list = registry.getIncomingNames().stream()
                    .map(channel -> prepareCloseIncoming(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(phaseTimeout());
            }
        } catch (Exception e) {
            log.warn("Error preparing close of incoming channels", e);
        }
    }

    // --- Prepare close outgoing channels ---

    @Override
    public void prepareCloseOutgoing() {
        if (!prepareCloseOutgoingDone.compareAndSet(false, true)) {
            return;
        }

        try {
            List<Uni<Void>> list = registry.getOutgoingNames().stream()
                    .map(channel -> prepareCloseOutgoing(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(phaseTimeout());
            }
        } catch (Exception e) {
            log.warn("Error preparing close of outgoing channels", e);
        }
    }

    // --- Close clients + terminate connectors ---

    @Override
    public void close() {
        if (!closeDone.compareAndSet(false, true)) {
            return;
        }

        // Taken before the channels are closed, which unregisters them: the connectors actually in use.
        // Connectors without channels may never have been created (their beans are created lazily), so they are left alone.
        Set<String> usedConnectors = new LinkedHashSet<>(registry.getConnectorNames().values());

        try {
            List<Uni<Void>> list = registry.getIncomingNames().stream()
                    .map(channel -> closeIncoming(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(phaseTimeout());
            }
        } catch (Exception e) {
            log.warn("Error closing incoming channels", e);
        }

        try {
            List<Uni<Void>> list = registry.getOutgoingNames().stream()
                    .map(channel -> closeOutgoing(channel)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(phaseTimeout());
            }
        } catch (Exception e) {
            log.warn("Error closing outgoing channels", e);
        }

        terminateConnectors(usedConnectors);
    }

    /**
     * CDI fallback: runs all three phases during CDI container destruction.
     * Each phase runs at most once: if an external orchestrator (e.g. Quarkus service graph) already started a phase,
     * it is skipped here. The guard is set when a phase starts, so a phase that is still running is skipped too.
     */
    public void onBeforeDestroyed(
            @Observes @Priority(40) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        prepareCloseIncoming();
        prepareCloseOutgoing();
        close();
    }

    // --- Per-channel prepare close (drain + connector prepare-close) ---

    @Override
    public Uni<Void> prepareCloseIncoming(String channel) {
        String connectorName = registry.getIncomingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getInboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        // A channel configured with concurrency is drained as a whole (composite pausable under its name),
        // then each indexed connector channel is prepared.
        return drainChannel(channel)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> forEachConnectorChannel(incomingConnectorChannels(channel), connector::prepareCloseIncoming));
    }

    @Override
    public Uni<Void> prepareCloseOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return drainChannel(channel)
                .emitOn(Infrastructure.getDefaultWorkerPool())
                .call(() -> Uni.createFrom().completionStage(() -> connector.prepareCloseOutgoing(channel)));
    }

    // --- Per-channel close ---

    @Override
    public Uni<Void> closeIncoming(String channel) {
        String connectorName = registry.getIncomingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getInboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return forEachConnectorChannel(incomingConnectorChannels(channel), connector::closeIncoming)
                // The channel is closed: forget it, so a channel created at runtime can be created again.
                .invoke(() -> {
                    if (registry instanceof InternalChannelRegistry internal) {
                        internal.unregisterIncoming(channel);
                    }
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * @return the channel names the connector uses for the given incoming channel: the indexed channels for a channel
     *         configured with {@code concurrency}, the channel itself otherwise
     */
    private List<String> incomingConnectorChannels(String channel) {
        if (registry instanceof InternalChannelRegistry internal) {
            return internal.getIncomingConnectorChannels(channel);
        }
        return List.of(channel);
    }

    private static Uni<Void> forEachConnectorChannel(List<String> connectorChannels,
            Function<String, CompletionStage<Void>> hook) {
        if (connectorChannels.size() == 1) {
            String connectorChannel = connectorChannels.get(0);
            return Uni.createFrom().completionStage(() -> hook.apply(connectorChannel));
        }
        List<Uni<Void>> calls = connectorChannels.stream()
                .map(connectorChannel -> Uni.createFrom().completionStage(() -> hook.apply(connectorChannel)))
                .toList();
        return Uni.join().all(calls).andCollectFailures().replaceWithVoid();
    }

    @Override
    public Uni<Void> closeOutgoing(String channel) {
        String connectorName = registry.getOutgoingConnectorName(channel);
        ConnectorLifecycle connector = connectorFactories.getOutboundConnectors().get(connectorName);
        if (connector == null) {
            return Uni.createFrom().voidItem();
        }

        return Uni.createFrom().completionStage(() -> connector.closeOutgoing(channel))
                // The channel is closed: forget it, so a channel created at runtime can be created again.
                .invoke(() -> {
                    if (registry instanceof InternalChannelRegistry internal) {
                        internal.unregisterOutgoing(channel);
                    }
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * The timeout of each shutdown phase: the {@value #PHASE_TIMEOUT_PROPERTY} property if set, otherwise the longest
     * drain timeout of the pausable channels (channels are drained concurrently) plus a margin for the connector calls.
     *
     * @return the phase timeout
     */
    private Duration phaseTimeout() {
        Optional<Integer> configured = Optional.empty();
        if (configInstance != null && configInstance.isResolvable()) {
            configured = configInstance.get().getOptionalValue(PHASE_TIMEOUT_PROPERTY, Integer.class);
        }
        return phaseTimeout(configured, registry.getPausableChannels().values());
    }

    /**
     * @param configuredMillis the value of the {@value #PHASE_TIMEOUT_PROPERTY} property, if set
     * @param pausables the pausable channels to drain
     * @return the configured timeout, or the longest drain timeout plus {@link #PHASE_TIMEOUT_MARGIN}
     */
    static Duration phaseTimeout(Optional<Integer> configuredMillis, Collection<PausableChannel> pausables) {
        if (configuredMillis.isPresent()) {
            return Duration.ofMillis(configuredMillis.get());
        }
        Duration longestDrain = pausables.stream()
                .map(PausableChannel::getDrainTimeout)
                .max(Duration::compareTo)
                .orElse(DEFAULT_DRAIN_TIMEOUT);
        return longestDrain.plus(PHASE_TIMEOUT_MARGIN);
    }

    // --- Connector termination ---

    private void terminateConnectors(Collection<String> usedConnectors) {
        List<ConnectorLifecycle> connectors = connectorsToTerminate(usedConnectors,
                connectorFactories.getInboundConnectors(), connectorFactories.getOutboundConnectors());

        try {
            List<Uni<Void>> list = connectors.stream()
                    .map(c -> Uni.createFrom().completionStage(c::terminate)
                            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool()))
                    .toList();
            if (!list.isEmpty()) {
                Uni.join().all(list).usingConcurrencyOf(10)
                        .andCollectFailures()
                        .await().atMost(phaseTimeout());
            }
        } catch (Exception e) {
            log.warn("Error during connector termination", e);
        }
    }

    /**
     * The connectors to terminate: those of the given names, each once.
     * <p>
     * A connector implementing both {@link InboundConnector} and {@link OutboundConnector} (e.g. Kafka) is in both maps
     * under the same name. If the inbound connector of a name is also an {@link OutboundConnector}, it is the outbound
     * connector of that name too (another outbound bean with the same name would have been rejected at startup), so it
     * is terminated once. This doesn't rely on the equality of the CDI client proxies.
     *
     * @param usedConnectors the names of the connectors used by channels
     * @param inbound the inbound connectors, by name
     * @param outbound the outbound connectors, by name
     * @return the connectors to terminate
     */
    static List<ConnectorLifecycle> connectorsToTerminate(Collection<String> usedConnectors,
            Map<String, ? extends InboundConnector> inbound, Map<String, ? extends OutboundConnector> outbound) {
        List<ConnectorLifecycle> connectors = new ArrayList<>();
        for (String name : new LinkedHashSet<>(usedConnectors)) {
            InboundConnector in = inbound.get(name);
            OutboundConnector out = outbound.get(name);
            if (in != null) {
                connectors.add(in);
            }
            if (out != null && !(in instanceof OutboundConnector)) {
                connectors.add(out);
            }
        }
        return connectors;
    }

    // --- Channel drain ---

    private Uni<Void> drainChannel(String channel) {
        PausableChannel pausable = registry.getPausable(channel);
        if (pausable == null) {
            return Uni.createFrom().voidItem();
        }
        try {
            Duration timeout = pausable.getDrainTimeout();
            return pausable.pauseAndDrain()
                    .ifNoItem().after(timeout).fail()
                    .onFailure().invoke(failure -> {
                        if (failure instanceof TimeoutException) {
                            ProviderLogging.log.drainTimedOut(channel, timeout, inFlight(pausable));
                        } else {
                            log.warnf(failure, "Failed to drain channel '%s'", channel);
                        }
                    })
                    .onFailure().recoverWithNull();
        } catch (Exception e) {
            log.warnf(e, "Failed to drain channel '%s'", channel);
            return Uni.createFrom().voidItem();
        }
    }

    private static String inFlight(PausableChannel pausable) {
        int count = CompositePausableChannel.inFlightCount(pausable);
        return count < 0 ? "unknown" : String.valueOf(count);
    }
}
