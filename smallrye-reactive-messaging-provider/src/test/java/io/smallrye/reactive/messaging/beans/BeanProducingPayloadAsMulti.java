package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanProducingPayloadAsMulti {

    @Outgoing("sink")
    public Multi<String> publisher() {
        return Multi.createFrom().range(1, 11).flatMap(i -> AdaptersToFlow.publisher(Flowable.just(i, i)))
                .map(i -> Integer.toString(i));
    }

}
