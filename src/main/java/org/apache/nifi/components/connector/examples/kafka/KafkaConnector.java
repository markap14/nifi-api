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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.examples.kafka.KafkaConnectivityProperties.SecurityProtocol;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An example of a Connector that reads from Kafka and writes to Snowflake. This Connector
 * is not expected to be provided as part of the API, but rather as an example of how
 * a Connector can be implemented, in order to both demonstrate the concepts and validate
 * the API itself.
 */
public class KafkaConnector extends AbstractConnector {
    private static final Map<String, ConnectorPropertyGroup> propertyGroups = new LinkedHashMap<>();
    static {
        propertyGroups.put(KafkaConnectivityProperties.KAFKA_CONNECTION_PROPERTY_GROUP.getName(), KafkaConnectivityProperties.KAFKA_CONNECTION_PROPERTY_GROUP);
        propertyGroups.put(SourceDataProperties.SOURCE_DATA_GROUP.getName(), SourceDataProperties.SOURCE_DATA_GROUP);
        propertyGroups.put(SnowflakeProperties.SNOWFLAKE_PROPERTY_GROUP.getName(), SnowflakeProperties.SNOWFLAKE_PROPERTY_GROUP);
    }

    @Override
    protected void init() throws FlowUpdateException {
        onConfigured();
    }

    @Override
    public List<String> getPropertyGroupNames() {
        final List<String> groupNames = new ArrayList<>();
        groupNames.add(KafkaConnectivityProperties.KAFKA_CONNECTION_PROPERTY_GROUP.getName());
        groupNames.add(SourceDataProperties.SOURCE_DATA_GROUP.getName());

        final String dataFormat = getProperty(SourceDataProperties.SOURCE_DATA_GROUP, SourceDataProperties.DATA_FORMAT);
        if ("AVRO".equalsIgnoreCase(dataFormat)) {
            groupNames.add(SchemaRegistryProperties.SCHEMA_REGISTRY_GROUP.getName());
        }

        groupNames.add(SnowflakeProperties.SNOWFLAKE_PROPERTY_GROUP.getName());
        return groupNames;
    }

    @Override
    public ConnectorPropertyGroup getPropertyGroup(final String groupName) {
        return Optional.ofNullable(propertyGroups.get(groupName))
            .orElseThrow(() -> new IllegalArgumentException("Unknown group name: " + groupName));
    }

    @Override
    public void onConfigured() throws FlowUpdateException {
        try {
            final VersionedProcessGroup rootGroup = buildFlowDefinition();
            getInitializationContext().updateFlow(rootGroup, this::drainFlowFiles);
        } catch (final IOException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public void onPropertyGroupConfigured(final String groupName) {

    }

    @Override
    public List<ValidationResult> validatePropertyGroup(final String groupName, final Map<String, String> propertyValues) {
        return List.of();
    }

    @Override
    protected void ensureDrainageUnblocked() throws InvocationFailedException {
        final List<ProcessorFacade> mergeProcessors = getInitializationContext().getRootGroup().getProcessors().stream()
            .filter(this::isDrainageBlock)
            .toList();

        for (final ProcessorFacade mergeProcessor : mergeProcessors) {
            mergeProcessor.invokeConnectorMethod("ignoreThresholds", Map.of());
        }
    }

    private boolean isDrainageBlock(final ProcessorFacade processor) {
        return processor.getDefinition().getType().contains("Merge");
    }

    private VersionedProcessGroup buildFlowDefinition() throws IOException {
        final String securityProtocol = getInitializationContext().getConfigurationContext().getProperty(
            KafkaConnectivityProperties.KAFKA_CONNECTION_PROPERTY_GROUP, KafkaConnectivityProperties.SECURITY_PROTOCOL);
        final String sourceGroupResourceName = getSourceGroupResourceName(securityProtocol);

        final VersionedProcessGroup sourceGroupFlow = readFlowDefinition(sourceGroupResourceName);
        final VersionedProcessGroup destinationFlow = readFlowDefinition("snowflake-destination.json");
        final VersionedConnection connection = createConnection(sourceGroupFlow, "Output", destinationFlow, "Input");

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setName("Kafka Connector Flow");
        rootGroup.setIdentifier("Kafka Root Group");
        rootGroup.setProcessGroups(Set.of(sourceGroupFlow, destinationFlow));
        connection.setGroupIdentifier(rootGroup.getIdentifier());
        rootGroup.setConnections(Set.of(connection));

        return rootGroup;
    }

    private static String getSourceGroupResourceName(final String securityProtocol) {
        if (securityProtocol.equals(SecurityProtocol.PLAINTEXT.name())) {
            return "kafka-plaintext.json";
        } else if (securityProtocol.equals(SecurityProtocol.SSL.name())) {
            return "kafka-ssl.json";
        } else if (securityProtocol.equals(SecurityProtocol.SASL_SSL.name())) {
            return "kafka-sasl.json";
        }

        throw new IllegalArgumentException("Unsupported security protocol: " + securityProtocol);
    }


    private VersionedProcessGroup readFlowDefinition(final String resourceName) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            final String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            getLogger().debug("Flow Definition: {}", json);
            return null;
            //return OBJECT_MAPPER.readValue(in, VersionedProcessGroup.class);
        }
    }

}
