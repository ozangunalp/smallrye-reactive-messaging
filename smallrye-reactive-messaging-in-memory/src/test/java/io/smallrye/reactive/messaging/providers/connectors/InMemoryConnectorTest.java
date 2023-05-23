package io.smallrye.reactive.messaging.providers.connectors;

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.memory.InMemoryConnector;
import io.smallrye.reactive.messaging.memory.InMemorySink;
import io.smallrye.reactive.messaging.memory.InMemorySource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class InMemoryConnectorTest extends WeldTestBase {

    @BeforeEach
    public void install() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("mp.messaging.incoming.foo.connector", InMemoryConnector.CONNECTOR);
        conf.put("mp.messaging.incoming.foo.data", "not read");
        conf.put("mp.messaging.outgoing.bar.connector", InMemoryConnector.CONNECTOR);
        conf.put("mp.messaging.outgoing.bar.data", "not read");
        installConfig(new MapBasedConfig(conf));
    }

    @AfterEach
    public void cleanup() {
        releaseConfig();
    }

    @Test
    public void testWithStrings() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySink<String> bar = bean.sink("bar");
        InMemorySource<String> foo = bean.source("foo");
        foo.send("hello");
        assertThat(bar.received()).hasSize(1).extracting(Message::getPayload).containsExactly("HELLO");
    }

    @Test
    public void testWithMessages() {
        addBeanClass(MyBeanReceivingMessage.class);
        initialize();
        AtomicBoolean acked = new AtomicBoolean();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySource<Message<String>> foo = bean.source("foo");
        InMemorySink<String> bar = bean.sink("bar");

        Message<String> msg = Message.of("hello", () -> {
            acked.set(true);
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        });
        foo.send(msg);
        assertThat(bar.received()).hasSize(1).extracting(Message::getPayload).containsExactly("HELLO");
        assertThat(acked).isTrue();
    }

    @Test
    public void testWithMultiplePayloads() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySource<String> foo = bean.source("foo");
        InMemorySink<String> bar = bean.sink("bar");
        foo.send("1");
        foo.send("2");
        foo.send("3");
        assertThat(bar.received()).hasSize(3).extracting(Message::getPayload).containsExactly("1", "2", "3");
        bar.clear();
        foo.send("4");
        foo.send("5");
        foo.send("6");
        foo.complete();

        assertThat(bar.received()).hasSize(3).extracting(Message::getPayload).containsExactly("4", "5", "6");
        assertThat(bar.hasCompleted()).isTrue();
        assertThat(bar.hasFailed()).isFalse();
    }

    @Test
    public void testWithFailure() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        InMemorySource<String> foo = bean.source("foo");
        InMemorySink<String> bar = bean.sink("bar");
        foo.send("1");
        foo.send("2");
        foo.send("3");
        assertThat(bar.received()).hasSize(3).extracting(Message::getPayload).containsExactly("1", "2", "3");
        foo.fail(new Exception("boom"));

        assertThat(bar.hasCompleted()).isFalse();
        assertThat(bar.hasFailed()).isTrue();
        assertThat(bar.getFailure()).hasMessageContaining("boom");

        bar.clear();
        assertThat(bar.hasCompleted()).isFalse();
        assertThat(bar.hasFailed()).isFalse();
        assertThat(bar.getFailure()).isNull();

    }

    @Test
    public void testWithUnknownSource() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        assertThatThrownBy(() -> bean.source("unknown")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWithUnknownSink() {
        addBeanClass(MyBeanReceivingString.class);
        initialize();
        InMemoryConnector bean = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();
        assertThat(bean).isNotNull();
        assertThatThrownBy(() -> bean.sink("unknown")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSwitchAndClear() {
        assertThat(System.getProperties()).doesNotContainKeys(
                "mp.messaging.incoming.a.connector", "mp.messaging.incoming.b.connector",
                "mp.messaging.outgoing.x.connector", "mp.messaging.outgoing.y.connector");

        InMemoryConnector.switchIncomingChannelsToInMemory("a", "b");
        InMemoryConnector.switchOutgoingChannelsToInMemory("x", "y");
        assertThat(System.getProperties())
                .contains(entry("mp.messaging.incoming.a.connector", InMemoryConnector.CONNECTOR));
        assertThat(System.getProperties())
                .contains(entry("mp.messaging.incoming.b.connector", InMemoryConnector.CONNECTOR));
        assertThat(System.getProperties())
                .contains(entry("mp.messaging.outgoing.x.connector", InMemoryConnector.CONNECTOR));
        assertThat(System.getProperties())
                .contains(entry("mp.messaging.outgoing.y.connector", InMemoryConnector.CONNECTOR));

        InMemoryConnector.clear();

        assertThat(System.getProperties()).doesNotContainKeys(
                "mp.messaging.incoming.a.connector", "mp.messaging.incoming.b.connector",
                "mp.messaging.outgoing.x.connector", "mp.messaging.outgoing.y.connector");
    }

    @Test
    public void testSwitchOnApplication() {
        addBeanClass(MyBeanReceivingString.class);
        Map<String, String> map1 = InMemoryConnector.switchIncomingChannelsToInMemory("foo");
        Map<String, String> map2 = InMemoryConnector.switchOutgoingChannelsToInMemory("bar");
        initialize();

        assertThat(map1).containsExactly(entry("mp.messaging.incoming.foo.connector", InMemoryConnector.CONNECTOR));
        assertThat(map2).containsExactly(entry("mp.messaging.outgoing.bar.connector", InMemoryConnector.CONNECTOR));

        InMemoryConnector connector = container.getBeanManager().createInstance()
                .select(InMemoryConnector.class, ConnectorLiteral.of(InMemoryConnector.CONNECTOR)).get();

        connector.source("foo").send("hello");
        assertThat(connector.sink("bar").received().stream()
                .map(Message::getPayload).collect(Collectors.toList())).containsExactly("HELLO");
    }

    @ApplicationScoped
    public static class MyBeanReceivingString {

        @Incoming("foo")
        @Outgoing("bar")
        public String process(String s) {
            return s.toUpperCase();
        }

    }

    @ApplicationScoped
    public static class MyBeanReceivingMessage {

        @Incoming("foo")
        @Outgoing("bar")
        public Message<String> process(Message<String> s) {
            return s.withPayload(s.getPayload().toUpperCase());
        }

    }

}
