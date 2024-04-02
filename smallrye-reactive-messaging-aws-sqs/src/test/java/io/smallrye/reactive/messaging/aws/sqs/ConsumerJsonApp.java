package io.smallrye.reactive.messaging.aws.sqs;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConsumerJsonApp {

    List<Person> received = new CopyOnWriteArrayList<>();

    @Incoming("data")
    void consume(Person msg) {
        received.add(msg);
    }

    public List<Person> received() {
        return received;
    }

    public static class Person {
        public String name;
        public int age;

    }

}
