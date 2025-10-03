/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class KafkaConnectivityProperties {

    private KafkaConnectivityProperties() {
    }

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

    static final ConnectorPropertyGroup KAFKA_BOOTSTRAP_SERVERS_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Bootstrap Servers")
        .description("Properties for connecting to Kafka Bootstrap Servers")
        .properties(BOOTSTRAP_SERVERS_PROPERTIES)
        .build();

    static final ConnectorPropertyGroup KAFKA_AUTHENTICATION_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Authentication")
        .description("Properties for authenticating to Kafka")
        .properties(AUTHENTICATION_PROPERTIES)
        .build();

    static final ConfigurationStep KAFKA_CONNECTION_STEP = new ConfigurationStep.Builder()
        .name("Kafka Connectivity")
        .description("Properties for connecting to Kafka")
        .propertyGroups(List.of(
            KAFKA_BOOTSTRAP_SERVERS_GROUP,
            KAFKA_AUTHENTICATION_GROUP
        ))
        .build();

}
