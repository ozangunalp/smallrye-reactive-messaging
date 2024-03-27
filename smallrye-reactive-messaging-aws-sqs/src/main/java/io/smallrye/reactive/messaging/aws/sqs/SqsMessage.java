package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import software.amazon.awssdk.services.sqs.model.Message;

public class SqsMessage implements ContextAwareMessage<String>, MetadataInjectableMessage<String> {

    private final Message message;
    private final SqsAckHandler ackHandler;
    private Metadata metadata;

    public SqsMessage(Message message, SqsAckHandler ackHandler) {
        this.message = message;
        this.ackHandler = ackHandler;
        this.metadata = captureContextMetadata(new SqsIncomingMetadata(message));
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String getPayload() {
        return message.body();
    }

    public Message getMessage() {
        return message;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        return ackHandler.handle(this).subscribeAsCompletionStage();
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        CompletableFuture<Void> nack = new CompletableFuture<>();
        runOnMessageContext(() -> nack.complete(null));
        return nack;
    }

    @Override
    public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return this::nack;
    }

    @Override
    public void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
