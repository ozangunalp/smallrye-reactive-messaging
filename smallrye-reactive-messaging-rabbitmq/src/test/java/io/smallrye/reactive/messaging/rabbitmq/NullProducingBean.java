package io.smallrye.reactive.messaging.rabbitmq;

import java.time.Duration;
import java.util.concurrent.Flow.Publisher;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

/**
 * A bean that can be registered to support publishing of messages to an
 * outgoing rabbitmq channel.
 */
@ApplicationScoped
public class NullProducingBean {

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        return Message.of(null, input::ack);
    }

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Multi.createFrom().ticks().every(Duration.ofMillis(100))
                .map(l -> l.intValue())
                .onItem().invoke(l -> {
                    if (l > 9) {
                        throw new IllegalArgumentException("Done");
                    }
                });
    }

}
