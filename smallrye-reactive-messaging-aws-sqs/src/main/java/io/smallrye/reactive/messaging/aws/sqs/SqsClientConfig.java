package io.smallrye.reactive.messaging.aws.sqs;

import java.util.Objects;

import io.smallrye.reactive.messaging.aws.sqs.i18n.AwsSqsLogging;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

public class SqsClientConfig {

    private final String queueName;
    private final Region region;
    private final String endpointOverride;
    private final String credentialsProviderClassName;

    public SqsClientConfig(SqsConnectorCommonConfiguration config) {
        this.queueName = config.getQueue().orElse(config.getChannel());
        this.region = config.getRegion().map(s -> {
            try {
                return Region.of(s);
            } catch (IllegalArgumentException e) {
                AwsSqsLogging.log.failedToParseAwsRegion(s, e.getMessage());
                return null;
            }
        }).orElse(null);
        this.endpointOverride = config.getEndpointOverride().orElse(null);
        this.credentialsProviderClassName = config.getCredentialsProvider().orElse(null);
    }

    public boolean isComplete() {
        return queueName != null && region != null;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getEndpointOverride() {
        return endpointOverride;
    }

    public Region getRegion() {
        return region;
    }

    public String getCredentialsProviderClassName() {
        return credentialsProviderClassName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (SqsClientConfig) o;
        return Objects.equals(endpointOverride, that.endpointOverride) &&
                Objects.equals(region, that.region) &&
                Objects.equals(queueName, that.queueName) &&
                Objects.equals(credentialsProviderClassName, that.credentialsProviderClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpointOverride, region, queueName, credentialsProviderClassName);
    }

    public AwsCredentialsProvider createCredentialsProvider() {
        var className = credentialsProviderClassName == null
                ? "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
                : credentialsProviderClassName;
        try {
            var clazz = (Class<? extends AwsCredentialsProvider>) Class.forName(className);
            var method = clazz.getMethod("create");
            return (AwsCredentialsProvider) method.invoke(null);
        } catch (Exception e) {
            AwsSqsLogging.log.failedToLoadAwsCredentialLoader(e.getMessage());
            return DefaultCredentialsProvider.create();
        }
    }
}