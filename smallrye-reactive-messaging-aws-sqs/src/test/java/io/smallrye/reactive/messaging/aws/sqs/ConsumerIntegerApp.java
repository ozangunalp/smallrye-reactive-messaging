package io.smallrye.reactive.messaging.aws.sqs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConsumerIntegerApp {

    List<Integer> received = new CopyOnWriteArrayList<>();

    @Incoming("data")
    void consume(int msg) {
        received.add(msg);
    }

    public List<Integer> received() {
        return received;
    }
}
