package io.smallrye.reactive.messaging.inject;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.TableView;

@ApplicationScoped
public class BeanInjectedWithATable {

    @Inject
    @Channel("hello")
    private MutinyEmitter<String> helloEmitter;

    @Inject
    @Channel("hello")
    private TableView<String, String> hello;

    @Inject
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
}
