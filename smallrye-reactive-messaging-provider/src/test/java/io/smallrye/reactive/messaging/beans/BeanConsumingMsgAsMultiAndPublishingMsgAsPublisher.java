package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow.Publisher;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanConsumingMsgAsMultiAndPublishingMsgAsPublisher {

    @Incoming("count")
    @Outgoing("sink")
    public Publisher<Message<String>> process(Multi<Message<Integer>> source) {
        return source
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> AdaptersToFlow.publisher(Flowable.just(i, i)))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
