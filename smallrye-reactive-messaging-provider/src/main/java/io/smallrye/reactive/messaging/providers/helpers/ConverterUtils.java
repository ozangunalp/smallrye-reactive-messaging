package io.smallrye.reactive.messaging.providers.helpers;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MessageConverter;

public class ConverterUtils {

    private ConverterUtils() {
        // Avoid direct instantiation.
    }

    public static Multi<? extends Message<?>> convert(Multi<? extends Message<?>> upstream,
            Instance<MessageConverter> converters, Type injectedPayloadType) {
        if (injectedPayloadType != null) {
            ConverterFunction converterFunction = new ConverterFunction(injectedPayloadType, converters);
            return upstream.onItem().transformToUniAndConcatenate(message -> {
                try {
                    Message<?> converted = converterFunction.apply(message);
                    return Uni.createFrom().item(converted);
                } catch (Throwable t) {
                    return Uni.createFrom().completionStage(() -> message.nack(t))
                            .replaceWith(Uni.createFrom().nullItem());
                }
            });
        }
        return upstream;
    }

    private static List<MessageConverter> getSortedConverters(Instance<MessageConverter> converters) {
        if (converters.isUnsatisfied()) {
            return Collections.emptyList();
        }

        return converters.stream().sorted(new Comparator<MessageConverter>() { // NOSONAR
            @Override
            public int compare(MessageConverter si1, MessageConverter si2) {
                int p1 = 0;
                int p2 = 0;
                if (si1 instanceof Prioritized) {
                    p1 = ((Prioritized) si1).getPriority();
                }
                if (si2 instanceof Prioritized) {
                    p2 = ((Prioritized) si2).getPriority();
                }
                if (si1.equals(si2)) {
                    return 0;
                }
                return Integer.compare(p1, p2);
            }
        }).collect(Collectors.toList());
    }

    private static class ConverterFunction implements Function<Message<?>, Message<?>> {

        private final Type injectedPayloadType;

        private final Instance<MessageConverter> converters;

        // cached message converter
        MessageConverter actual;

        public ConverterFunction(Type injectedPayloadType, Instance<MessageConverter> converters) {
            this.injectedPayloadType = injectedPayloadType;
            this.converters = converters;
        }

        @Override
        public Message<?> apply(Message<?> o) {
            //noinspection ConstantConditions - it can be `null`
            if (injectedPayloadType == null) {
                return o;
            } else if (o.getPayload() != null && o.getPayload().getClass().equals(injectedPayloadType)) {
                return o;
            }

            if (actual != null) {
                // Use the cached converter.
                return actual.convert(o, injectedPayloadType);
            } else {
                if (o.getPayload() != null
                        && TypeUtils.isAssignable(o.getPayload().getClass(), injectedPayloadType)) {
                    actual = MessageConverter.IdentityConverter.INSTANCE;
                    return o;
                }
                // Lookup and cache
                for (MessageConverter conv : getSortedConverters(converters)) {
                    if (conv.canConvert(o, injectedPayloadType)) {
                        actual = conv;
                        return actual.convert(o, injectedPayloadType);
                    }
                }
                // No converter found
                return o;
            }
        }
    }
}
