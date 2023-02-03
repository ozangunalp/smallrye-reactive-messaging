package io.smallrye.reactive.messaging.amqp;

import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class ProducingBean {

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        return Message.of(input.getPayload() + 1, input::ack);
    }

    @Outgoing("data")
    public Flow.Publisher<Integer> source() {
        return Multi.createFrom().range(0, 10);
    }

}
