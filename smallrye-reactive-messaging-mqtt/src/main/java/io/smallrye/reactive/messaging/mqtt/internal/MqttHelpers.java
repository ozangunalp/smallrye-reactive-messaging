package io.smallrye.reactive.messaging.mqtt.internal;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttExceptions.ex;
import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.time.Duration;
import java.util.Optional;
import java.util.regex.Pattern;

import jakarta.enterprise.inject.Instance;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.mqtt.MqttConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.mqtt.session.ConstantReconnectDelayOptions;
import io.smallrye.reactive.messaging.mqtt.session.MqttClientSessionOptions;
import io.smallrye.reactive.messaging.mqtt.session.ReconnectDelayOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.net.TrustOptions;

public class MqttHelpers {

    private MqttHelpers() {
        // avoid direct instantiation.
    }

    private static MqttClientSessionOptions createMqttClientOptions(MqttConnectorCommonConfiguration config) {
        MqttClientSessionOptions options = new MqttClientSessionOptions();
        options.setCleanSession(config.getAutoCleanSession());
        options.setAutoGeneratedClientId(config.getAutoGeneratedClientId());
        options.setAutoKeepAlive(config.getAutoKeepAlive());
        options.setClientId(config.getClientId().orElse(null));
        options.setConnectTimeout(config.getConnectTimeoutSeconds() * 1000);
        options.setHostname(config.getHost());
        options.setKeepAliveInterval(config.getKeepAliveSeconds());
        options.setMaxInflightQueue(config.getMaxInflightQueue());
        options.setMaxMessageSize(config.getMaxMessageSize());
        options.setPassword(config.getPassword().orElse(null));
        options.setPort(config.getPort().orElseGet(() -> config.getSsl() ? 8883 : 1883));
        options.setReconnectDelay(getReconnectDelayOptions(config));
        options.setSsl(config.getSsl());
        options.setKeyCertOptions(getKeyCertOptions(config));
        options.setServerName(config.getServerName());
        options.setTrustOptions(getTrustOptions(config));
        options.setTrustAll(config.getTrustAll());
        options.setUsername(config.getUsername().orElse(null));
        options.setWillQoS(config.getWillQos());
        options.setWillFlag(config.getWillFlag());
        options.setWillRetain(config.getWillRetain());
        options.setUnsubscribeOnDisconnect(config.getUnsubscribeOnDisconnection());
        return options;
    }

    public static MqttClientSessionOptions createClientOptions(MqttConnectorCommonConfiguration config,
            Instance<MqttClientSessionOptions> mqttClientOptions) {
        MqttClientSessionOptions options;
        Optional<String> clientOptionsName = config.getClientOptionsName();
        if (clientOptionsName.isPresent()) {
            options = createClientOptionsFromClientOptionsBean(mqttClientOptions, clientOptionsName.get(), config);
        } else {
            options = createMqttClientOptions(config);
        }
        return options;
    }

    static MqttClientSessionOptions createClientOptionsFromClientOptionsBean(Instance<MqttClientSessionOptions> instance,
            String optionsBeanName, MqttConnectorCommonConfiguration config) {
        Instance<MqttClientSessionOptions> options = instance.select(Identifier.Literal.of(optionsBeanName));
        if (options.isUnsatisfied()) {
            throw ex.illegalStateFindingBean(MqttClientSessionOptions.class.getName(), optionsBeanName);
        }
        log.createClientFromBean(optionsBeanName);

        // We must merge the channel config and the AMQP Client options.
        // In case of conflict, use the channel config.
        MqttClientSessionOptions customizerOptions = options.get();
        merge(customizerOptions, config);
        return customizerOptions;
    }

    /**
     * Merges the values from {@code channel} (the channel configuration), into the {@code custom}.
     * Values from {@code channel} replaces the values from {@code custom}.
     *
     * @param custom the custom configuration
     * @param config the common configuration
     */
    static void merge(MqttClientSessionOptions custom, MqttConnectorCommonConfiguration config) {
        MqttClientSessionOptions channel = createMqttClientOptions(config);
        String hostFromChannel = channel.getHostname();
        int portFromChannel = channel.getPort();

        if (isSetInChannelConfiguration("username", config)) {
            custom.setUsername(channel.getUsername());
        }

        // If the username is not set, use the one from the channel (alias or connector config)
        if (custom.getUsername() == null) {
            custom.setUsername(channel.getUsername());
        }

        if (isSetInChannelConfiguration("password", config)) {
            custom.setPassword(channel.getPassword());
        }

        // If the password is not set, use the one from the channel (alias or connector config)
        if (custom.getPassword() == null) {
            custom.setPassword(channel.getPassword());
        }

        if (isSetInChannelConfiguration("host", config)) {
            custom.setHostname(hostFromChannel);
        }

        // If the host is not set, use the one from the channel (alias or connector config)
        if (custom.getHostname() == null) {
            custom.setHostname(channel.getHostname());
        }

        if (isSetInChannelConfiguration("port", config)) {
            custom.setPort(portFromChannel);
        }

        // custom.getPort() will return a value (might be the default port, or not, or the default port as set by the user)
        // the channel port may be set (using the alias or connector config)
        // we can compare, but we cannot decide which one is going to be used.
        // so, we use the one from the customizer except is explicitly set on the channel.
        // The same apply for the rest of the options.

        if (isSetInChannelConfiguration("ssl", config)) {
            custom.setSsl(channel.isSsl());
        }

        if (isSetInChannelConfiguration("reconnect-attempts", config)) {
            custom.setReconnectAttempts(channel.getReconnectAttempts());
        }
        if (isSetInChannelConfiguration("reconnect-interval", config)) {
            custom.setReconnectInterval(channel.getReconnectInterval());
        }
        if (isSetInChannelConfiguration("connect-timeout", config)) {
            custom.setConnectTimeout(channel.getConnectTimeout());
        }

        if (isSetInChannelConfiguration("auto-clean-session", config)) {
            custom.setCleanSession(config.getAutoCleanSession());
        }

        if (isSetInChannelConfiguration("auto-generated-client-id", config)) {
            custom.setAutoGeneratedClientId(config.getAutoGeneratedClientId());
        }

        if (isSetInChannelConfiguration("auto-keep-alive", config)) {
            custom.setAutoKeepAlive(config.getAutoKeepAlive());
        }

        if (isSetInChannelConfiguration("client-id", config)) {
            custom.setClientId(config.getClientId().orElse(null));
        }

        if (isSetInChannelConfiguration("keep-alive-interval", config)) {
            custom.setKeepAliveInterval(config.getKeepAliveSeconds());
        }

        if (isSetInChannelConfiguration("max-inflight-queue", config)) {
            custom.setMaxInflightQueue(config.getMaxInflightQueue());
        }

        if (isSetInChannelConfiguration("max-message-size", config)) {
            custom.setMaxMessageSize(config.getMaxMessageSize());
        }

        if (isSetInChannelConfiguration("reconnect-interval-seconds", config)) {
            custom.setReconnectDelay(getReconnectDelayOptions(config));
        }

        if (isSetInChannelConfiguration("ssl-keystore-location", config)) {
            custom.setKeyCertOptions(getKeyCertOptions(config));
        }

        if (isSetInChannelConfiguration("server-name", config)) {
            custom.setServerName(config.getServerName());
        }

        if (isSetInChannelConfiguration("ssl-truststore-location", config)) {
            custom.setTrustOptions(getTrustOptions(config));
        }

        if (isSetInChannelConfiguration("trust-all", config)) {
            custom.setTrustAll(config.getTrustAll());
        }

        if (isSetInChannelConfiguration("will-qus", config)) {
            custom.setWillQoS(config.getWillQos());
        }

        if (isSetInChannelConfiguration("will-flag", config)) {
            custom.setWillFlag(config.getWillFlag());
        }

        if (isSetInChannelConfiguration("will-retain", config)) {
            custom.setWillRetain(config.getWillRetain());
        }
        if (isSetInChannelConfiguration("unsubscribe-on-disconnection", config)) {
            custom.setUnsubscribeOnDisconnect(config.getUnsubscribeOnDisconnection());
            ;
        }
    }

    /**
     * Create KeyCertOptions value from the configuration.
     * Attribute Name: ssl.keystore
     * Description: Set whether keystore type, location and password. In case of pem type the location and password are the cert
     * and key path.
     * Default Value: PfxOptions
     *
     * @return the KeyCertOptions
     */
    private static KeyCertOptions getKeyCertOptions(MqttConnectorCommonConfiguration config) {
        Optional<String> sslKeystoreLocation = config.getSslKeystoreLocation();
        Optional<String> sslKeystorePassword = config.getSslKeystorePassword();
        if (config.getSsl() && sslKeystoreLocation.isPresent()) {
            String keyStoreLocation = sslKeystoreLocation.get();
            String sslKeystoreType = config.getSslKeystoreType();

            if (sslKeystorePassword.isPresent()) {
                String keyStorePassword = sslKeystorePassword.get();
                if ("jks".equalsIgnoreCase(sslKeystoreType)) {
                    return new JksOptions()
                            .setPath(keyStoreLocation)
                            .setPassword(keyStorePassword);
                } else if ("pem".equalsIgnoreCase(sslKeystoreType)) {
                    return new PemKeyCertOptions()
                            .setCertPath(keyStoreLocation)
                            .setKeyPath(keyStorePassword);
                }
                // Default
                return new PfxOptions()
                        .setPath(keyStoreLocation)
                        .setPassword(keyStorePassword);
            } else {
                throw new IllegalArgumentException(
                        "The attribute `ssl.keystore.password` on connector 'smallrye-mqtt' (channel: "
                                + config.getChannel() + ") must be set for `ssl.keystore.type`" + sslKeystoreType);
            }
        }
        return null;
    }

    /**
     * Gets the truststore value from the configuration.
     * Attribute Name: ssl.truststore
     * Description: Set whether keystore type, location and password. In case of pem type the location is the cert path.
     * Default Value: PfxOptions
     *
     * @return the TrustOptions
     */

    private static TrustOptions getTrustOptions(MqttConnectorCommonConfiguration config) {
        Optional<String> sslTruststoreLocation = config.getSslTruststoreLocation();
        Optional<String> sslTruststorePassword = config.getSslTruststorePassword();
        if (config.getSsl() && sslTruststoreLocation.isPresent()) {
            String truststoreLocation = sslTruststoreLocation.get();
            String truststoreType = config.getSslTruststoreType();

            if ("pem".equalsIgnoreCase(truststoreType)) {
                return new PemTrustOptions()
                        .addCertPath(truststoreLocation);
            } else {
                if (sslTruststorePassword.isPresent()) {
                    String truststorePassword = sslTruststorePassword.get();
                    if ("jks".equalsIgnoreCase(truststoreType)) {
                        return new JksOptions()
                                .setPath(truststoreLocation)
                                .setPassword(truststorePassword);
                    }
                    // Default
                    return new PfxOptions()
                            .setPath(truststoreLocation)
                            .setPassword(truststorePassword);
                } else {
                    throw new IllegalArgumentException(
                            "The attribute `ssl.keystore.password` on connector 'smallrye-mqtt' (channel: "
                                    + config.getChannel() + ") must be set for `ssl.keystore.type`" + truststoreType);
                }
            }
        }
        return null;
    }

    private static ReconnectDelayOptions getReconnectDelayOptions(MqttConnectorCommonConfiguration config) {
        ConstantReconnectDelayOptions options = new ConstantReconnectDelayOptions();
        options.setDelay(Duration.ofSeconds(config.getReconnectIntervalSeconds()));
        return options;
    }

    /**
     * If topic starts with shared subscription like `$share/group/....`
     * we need to remove it `$share/group/` prefix
     */
    public static String rebuildMatchesWithSharedSubscription(String topic) {
        if (Pattern.matches("^\\$share/((?!/).)*/.*", topic)) {
            return topic.replaceAll("^\\$share/((?!/).)*/", "");
        } else {
            return topic;
        }
    }

    /**
     * Checks whether the given attribute is set in the channel configuration.
     * It does not check for aliases or into the global connector configuration.
     *
     * @param attribute the attribute
     * @param configuration the configuration object
     * @return {@code true} is the given attribute is configured in the channel configuration
     */
    static boolean isSetInChannelConfiguration(String attribute, MqttConnectorCommonConfiguration configuration) {
        return configuration.config().getConfigValue(attribute).getRawValue() != null;
    }

}