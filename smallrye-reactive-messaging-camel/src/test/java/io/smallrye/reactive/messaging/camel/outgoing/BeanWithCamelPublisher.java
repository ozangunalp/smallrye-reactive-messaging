package io.smallrye.reactive.messaging.camel.outgoing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.camel.Exchange;
import org.apache.camel.component.reactive.streams.api.CamelReactiveStreamsService;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanWithCamelPublisher {

    @Inject
    private CamelReactiveStreamsService camel;

    private final List<String> values = new ArrayList<>();

    @Incoming("sink")
    public CompletionStage<Void> sink(String value) {
        values.add(value);
        return CompletableFuture.completedFuture(null);
    }

    @Incoming("camel")
    @Outgoing("sink")
    public String extract(Exchange exchange) {
        return exchange.getIn().getBody(String.class);
    }

    @Outgoing("camel")
    public Flow.Publisher<Exchange> source() {
        return AdaptersToFlow.publisher(camel.from("seda:camel"));
    }

    public List<String> values() {
        return values;
    }

}
