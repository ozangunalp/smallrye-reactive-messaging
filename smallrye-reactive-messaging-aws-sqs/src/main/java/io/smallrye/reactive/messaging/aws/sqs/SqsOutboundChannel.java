package io.smallrye.reactive.messaging.aws.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsOutboundChannel {

    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final SqsAsyncClient client;
    private final String channel;

    private final Uni<String> queueUrlUni;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final JsonMapping jsonMapping;

    public SqsOutboundChannel(SqsConnectorOutgoingConfiguration conf, SqsManager sqsManager, JsonMapping jsonMapping) {
        this.channel = conf.getChannel();
        this.client = sqsManager.getClient(conf);
        this.queueUrlUni = sqsManager.getQueueUrl(conf).memoize().indefinitely();
        this.jsonMapping = jsonMapping;
        this.subscriber = MultiUtils.via(multi -> multi
                .onSubscription().call(s -> queueUrlUni)
                .call(m -> publishMessage(this.client, m)));
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return subscriber;
    }

    private Uni<Void> publishMessage(SqsAsyncClient client, Message<?> m) {
        if (closed.get()) {
            return Uni.createFrom().voidItem();
        }
        if (m.getPayload() == null) {
            return Uni.createFrom().nullItem();
        }
        return queueUrlUni.map(queueUrl -> getSendMessageRequest(queueUrl, m))
                .chain(request -> Uni.createFrom().completionStage(() -> client.sendMessage(request)))
                .invoke(r -> AwsSqsLogging.log.messageSentToChannel(channel, r.messageId(), r.sequenceNumber()))
                .onItemOrFailure().transformToUni((response, t) -> {
                    if (t == null) {
                        OutgoingMessageMetadata.setResultOnMessage(m, response);
                        return Uni.createFrom().completionStage(m.ack());
                    } else {
                        return Uni.createFrom().completionStage(m.nack(t));
                    }
                });
    }

    private SendMessageRequest getSendMessageRequest(String channelQueueUrl, Message<?> m) {
        Object payload = m.getPayload();
        String queueUrl = channelQueueUrl;
        if (payload instanceof SendMessageRequest) {
            return (SendMessageRequest) payload;
        }
        if (payload instanceof SendMessageRequest.Builder) {
            return ((SendMessageRequest.Builder) payload)
                    .queueUrl(queueUrl)
                    .build();
        }
        SendMessageRequest.Builder builder = SendMessageRequest.builder();
        Map<String, MessageAttributeValue> msgAttributes = new HashMap<>();
        Optional<SqsOutboundMetadata> metadata = m.getMetadata(SqsOutboundMetadata.class);
        if (metadata.isPresent()) {
            SqsOutboundMetadata md = metadata.get();
            if (md.getQueueUrl() != null) {
                queueUrl = md.getQueueUrl();
            }
            if (md.getDeduplicationId() != null) {
                builder.messageDeduplicationId(md.getDeduplicationId());
            }
            if (md.getGroupId() != null) {
                builder.messageGroupId(md.getGroupId());
            }
            if (md.getDelaySeconds() != null) {
                builder.delaySeconds(md.getDelaySeconds());
            }
            if (md.getMessageAttributes() != null) {
                msgAttributes.putAll(md.getMessageAttributes());
            }
        }
        if (payload instanceof software.amazon.awssdk.services.sqs.model.Message) {
            software.amazon.awssdk.services.sqs.model.Message msg = (software.amazon.awssdk.services.sqs.model.Message) payload;
            if (msg.hasAttributes()) {
                msgAttributes.putAll(msg.messageAttributes());
            }
            return builder
                    .queueUrl(queueUrl)
                    .messageAttributes(msgAttributes)
                    .messageBody(msg.body())
                    .build();
        }
        String messageBody = outgoingPayloadClassName(payload, msgAttributes);
        return builder
                .queueUrl(queueUrl)
                .messageAttributes(msgAttributes)
                .messageBody(messageBody)
                .build();
    }

    private String outgoingPayloadClassName(Object payload, Map<String, MessageAttributeValue> messageAttributes) {
        if (payload instanceof String || payload.getClass().isPrimitive() || isPrimitiveBoxed(payload.getClass())) {
            messageAttributes.put(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder().dataType("String")
                    .stringValue(payload.getClass().getName()).build());
            return String.valueOf(payload);
        } else if (payload.getClass().isArray() && payload.getClass().getComponentType().equals(Byte.TYPE)) {
            return new String((byte[]) payload);
        } else if (jsonMapping != null) {
            messageAttributes.put(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder().dataType("String")
                    .stringValue(payload.getClass().getName()).build());
            return jsonMapping.toJson(payload);
        }
        return String.valueOf(payload);
    }

    private boolean isPrimitiveBoxed(Class<?> c) {
        return c.equals(Boolean.class)
                || c.equals(Integer.class)
                || c.equals(Byte.class)
                || c.equals(Double.class)
                || c.equals(Float.class)
                || c.equals(Short.class)
                || c.equals(Character.class)
                || c.equals(Long.class);
    }

    public void close() {
        closed.set(true);
    }
}
