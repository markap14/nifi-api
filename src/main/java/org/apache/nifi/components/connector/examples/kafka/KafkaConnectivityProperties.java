/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.ConnectorPropertySubGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class KafkaConnectivityProperties {

    static final ConnectorPropertyDescriptor BOOTSTRAP_SERVERS = new ConnectorPropertyDescriptor.Builder()
        .name("Bootstrap Servers")
        .description("Comma-separated list of Kafka Bootstrap Servers in the format host:port. Corresponds to Kafka bootstrap.servers property")
        .required(true)
        .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor SECURITY_PROTOCOL = new ConnectorPropertyDescriptor.Builder()
        .name("Security Protocol")
        .description("Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property")
        .required(true)
        .allowableValues(SecurityProtocol.values())
        .defaultValue(SecurityProtocol.PLAINTEXT.name())
        .build();

    static final ConnectorPropertyDescriptor SASL_MECHANISM = new ConnectorPropertyDescriptor.Builder()
        .name("SASL Mechanism")
        .description("SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property")
        .required(true)
        .allowableValues(SaslMechanism.getAvailableSaslMechanisms())
        .defaultValue(SaslMechanism.GSSAPI)
        .dependsOn(SECURITY_PROTOCOL,
            SecurityProtocol.SASL_PLAINTEXT.name(),
            SecurityProtocol.SASL_SSL.name())
        .build();

    static final ConnectorPropertyDescriptor SASL_USERNAME = new ConnectorPropertyDescriptor.Builder()
        .name("SASL Username")
        .description("Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms")
        .required(true)
        .dependsOn(
            SASL_MECHANISM,
            SaslMechanism.PLAIN,
            SaslMechanism.SCRAM_SHA_256,
            SaslMechanism.SCRAM_SHA_512
        )
        .build();

    static final ConnectorPropertyDescriptor SASL_PASSWORD = new ConnectorPropertyDescriptor.Builder()
        .name("SASL Password")
        .description("Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms")
        .required(true)
        .type(PropertyType.PASSWORD)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .dependsOn(
            SASL_MECHANISM,
            SaslMechanism.PLAIN,
            SaslMechanism.SCRAM_SHA_256,
            SaslMechanism.SCRAM_SHA_512
        )
        .build();

    static final ConnectorPropertyDescriptor TOKEN_AUTHENTICATION = new ConnectorPropertyDescriptor.Builder()
        .name("Token Authentication")
        .description("Enables or disables Token authentication when using SCRAM SASL Mechanisms")
        .required(false)
        .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .defaultValue(Boolean.FALSE.toString())
        .dependsOn(
            SASL_MECHANISM,
            SaslMechanism.SCRAM_SHA_256,
            SaslMechanism.SCRAM_SHA_512
        )
        .build();

    static final ConnectorPropertyDescriptor AWS_PROFILE_NAME = new ConnectorPropertyDescriptor.Builder()
        .name("AWS Profile Name")
        .description("The Amazon Web Services Profile to select when multiple profiles are available.")
        .dependsOn(
            SASL_MECHANISM,
            SaslMechanism.AWS_MSK_IAM
        )
        .required(false)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();


    public enum SecurityProtocol {
        PLAINTEXT,
        SSL,
        SASL_PLAINTEXT,
        SASL_SSL
    }

    private static final List<ConnectorPropertyDescriptor> BOOTSTRAP_SERVERS_PROPERTIES = List.of(
        BOOTSTRAP_SERVERS
    );

    private static final List<ConnectorPropertyDescriptor> AUTHENTICATION_PROPERTIES = List.of(
        SECURITY_PROTOCOL,
        SASL_MECHANISM,
        SASL_USERNAME,
        SASL_PASSWORD,
        TOKEN_AUTHENTICATION,
        AWS_PROFILE_NAME
    );

    static final ConnectorPropertySubGroup KAFKA_BOOTSTRAP_SERVERS_SUBGROUP = new ConnectorPropertySubGroup.Builder()
        .name("Kafka Bootstrap Servers")
        .description("Properties for connecting to Kafka Bootstrap Servers")
        .properties(BOOTSTRAP_SERVERS_PROPERTIES)
        .build();

    static final ConnectorPropertySubGroup KAFKA_AUTHENTICATION_SUBGROUP = new ConnectorPropertySubGroup.Builder()
        .name("Kafka Authentication")
        .description("Properties for authenticating to Kafka")
        .properties(AUTHENTICATION_PROPERTIES)
        .build();

    static final ConnectorPropertyGroup KAFKA_CONNECTION_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Connectivity")
        .description("Properties for connecting to Kafka")
        .subGroups(List.of(
            KAFKA_BOOTSTRAP_SERVERS_SUBGROUP,
            KAFKA_AUTHENTICATION_SUBGROUP
        ))
        .build();

}
