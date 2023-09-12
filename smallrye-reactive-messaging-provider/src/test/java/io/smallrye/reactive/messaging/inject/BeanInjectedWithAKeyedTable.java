package io.smallrye.reactive.messaging.inject;

import java.lang.reflect.Type;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.TableView;
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;
import io.smallrye.reactive.messaging.keyed.Keyed;

@ApplicationScoped
public class BeanInjectedWithAKeyedTable {

    @Inject
    @Channel("hello")
    private MutinyEmitter<String> helloEmitter;

    @Inject
    @Keyed(AppendingKeyExtractor.class)
    @Channel("hello")
    private TableView<String, String> hello;

    @Inject
    @Keyed(AppendingKeyExtractor.class)
    @Channel("bonjour")
    private TableView<String, String> bonjour;

    public Map<String, String> getMapBonjour() {
        return bonjour.fetchAll();
    }

    public TableView<String, String> getHello() {
        return hello;
    }

    public MutinyEmitter<String> getHelloEmitter() {
        return helloEmitter;
    }

    public TableView<String, String> getBonjour() {
        return bonjour;
    }

    @ApplicationScoped
    public static class AppendingKeyExtractor implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> first, Type keyType, Type valueType) {
            return true;
        }

        @Override
        public Object extractKey(Message<?> message, Type keyType) {
            return "k-" + message.getPayload();
        }

        @Override
        public Object extractValue(Message<?> message, Type valueType) {
            return message.getPayload();
        }
    }
}
