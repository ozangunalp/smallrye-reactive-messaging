package io.smallrye.reactive.messaging.aws.sqs;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConsumerApp {

    List<String> received = new CopyOnWriteArrayList<>();

    @Incoming("data")
    void consume(String msg) {
        received.add(msg);
    }

    public List<String> received() {
        return received;
    }
}
