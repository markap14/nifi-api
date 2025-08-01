/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorFlow;
import org.apache.nifi.components.connector.ConnectorParameterContext;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowMigration;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.examples.common.AbstractConnector;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

public class KafkaConnector extends AbstractConnector {

    static final String PLAINTEXT = "PLAINTEXT";
    static final String SSL = "SSL";
    static final String SASL_SSL = "SASL_SSL";

    static final ConnectorPropertyDescriptor BOOTSTRAP_SERVERS = new ConnectorPropertyDescriptor.Builder()
        .name("Bootstrap Servers")
        .description("The Kafka bootstrap servers to connect to")
        .defaultValue("localhost:9092")
        .required(true)
        .build();

    static final ConnectorPropertyDescriptor TOPIC = new ConnectorPropertyDescriptor.Builder()
        .name("Topic")
        .description("The Kafka topic to produce or consume messages from")
        .required(true)
        .build();

    static final ConnectorPropertyDescriptor SECURITY_PROTOCOL = new ConnectorPropertyDescriptor.Builder()
        .name("Security Protocol")
        .description("The security protocol to use for connecting to Kafka")
        .allowableValues(PLAINTEXT, SSL, SASL_SSL)
        .defaultValue(PLAINTEXT)
        .required(true)
        .build();

    static List<ConnectorPropertyDescriptor> kafkaProperties = List.of(
        BOOTSTRAP_SERVERS,
        TOPIC,
        SECURITY_PROTOCOL
    );

    static final ConnectorPropertyGroup kafkaPropertyGroup = new ConnectorPropertyGroup.Builder()
        .name("Kafka Configuration")
        .description("Properties for connecting to Kafka")
        .propertyDescriptors(kafkaProperties)
        .build();


    static final ConnectorPropertyDescriptor DATABASE = new ConnectorPropertyDescriptor.Builder()
        .name("Database")
        .description("The Snowflake database to connect to")
        .required(true)
        .build();
    static final ConnectorPropertyDescriptor SCHEMA = new ConnectorPropertyDescriptor.Builder()
        .name("Schema")
        .description("The Snowflake schema to use")
        .required(true)
        .build();
    static final ConnectorPropertyDescriptor WAREHOUSE = new ConnectorPropertyDescriptor.Builder()
        .name("Warehouse")
        .description("The Snowflake warehouse to use for processing")
        .required(true)
        .build();

    static List<ConnectorPropertyDescriptor> snowflakeProperties = List.of(
        DATABASE,
        SCHEMA,
        WAREHOUSE
    );

    static final ConnectorPropertyGroup snowflakePropertyGroup = new ConnectorPropertyGroup.Builder()
        .name("Snowflake Configuration")
        .description("Properties for connecting to Snowflake")
        .propertyDescriptors(snowflakeProperties)
        .build();



    @Override
    public ConnectorFlow getFlowDefinition() throws IOException {
        final String securityProtocol = getInitializationContext().getConfigurationContext().getProperty(SECURITY_PROTOCOL);
        final String sourceGroupResourceName = switch (securityProtocol) {
            case PLAINTEXT -> "kafka-plaintext.json";
            case SSL -> "kafka-ssl.json";
            case SASL_SSL -> "kafka-sasl.json";
            default -> throw new IllegalArgumentException("Unsupported security protocol: " + securityProtocol);
        };

        final VersionedProcessGroup sourceGroupFlow = readFlowDefinition(sourceGroupResourceName);
        final VersionedProcessGroup destinationFlow = readFlowDefinition("snowflake-destination.json");
        final VersionedConnection connection = createConnection(sourceGroupFlow, "Output",
            destinationFlow, "Input");

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setName("Kafka Connector Flow");
        rootGroup.setIdentifier("Kafka Root Group");
        rootGroup.setProcessGroups(Set.of(sourceGroupFlow, destinationFlow));
        connection.setGroupIdentifier(rootGroup.getIdentifier());
        rootGroup.setConnections(Set.of(connection));

        final ConnectorFlow flow = new ConnectorFlow() {
            @Override
            public VersionedProcessGroup getRootGroup() {
                return rootGroup;
            }

            @Override
            public ConnectorParameterContext getParameterContext() {
                return null;
            }
        };

        return flow;
    }


    private VersionedProcessGroup readFlowDefinition(final String resourceName) throws IOException {
        try (final InputStream in = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            return null;
            //return OBJECT_MAPPER.readValue(in, VersionedProcessGroup.class);
        }
    }

    @Override
    public FlowMigration getFlowMigration() {
        return null;
    }

    @Override
    public List<ConnectorPropertyGroup> getPropertyGroups() {
        return List.of(
            kafkaPropertyGroup,
            snowflakePropertyGroup
        );
    }

}
