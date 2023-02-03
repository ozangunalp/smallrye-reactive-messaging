package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanConsumingMsgAsRSPublisherAndPublishingMsgAsMulti {

    @Incoming("count")
    @Outgoing("sink")
    public Multi<Message<String>> process(Publisher<Message<Integer>> source) {
        return Multi.createFrom().publisher(AdaptersToFlow.publisher(source))
                .map(Message::getPayload)
                .map(i -> i + 1)
                .flatMap(i -> AdaptersToFlow.publisher(Flowable.just(i, i)))
                .map(i -> Integer.toString(i))
                .map(Message::of);
    }

}
