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
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.examples.kafka.KafkaConnectivityProperties.SecurityProtocol;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An example of a Connector that reads from Kafka and writes to Snowflake. This Connector
 * is not expected to be provided as part of the API, but rather as an example of how
 * a Connector can be implemented, in order to both demonstrate the concepts and validate
 * the API itself.
 */
public class KafkaConnector extends AbstractConnector {
    private static final Map<String, ConfigurationStep> configurationSteps = new LinkedHashMap<>();
    static {
        configurationSteps.put(KafkaConnectivityProperties.KAFKA_CONNECTION_STEP.getName(), KafkaConnectivityProperties.KAFKA_CONNECTION_STEP);
        configurationSteps.put(SourceDataProperties.SOURCE_DATA_STEP.getName(), SourceDataProperties.SOURCE_DATA_STEP);
        configurationSteps.put(SnowflakeProperties.SNOWFLAKE_STEP.getName(), SnowflakeProperties.SNOWFLAKE_STEP);
    }

    @Override
    protected void init() throws FlowUpdateException {
        finishUpdate();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        final List<ConfigurationStep> steps = new ArrayList<>();
        steps.add(KafkaConnectivityProperties.KAFKA_CONNECTION_STEP);
        steps.add(SourceDataProperties.SOURCE_DATA_STEP);

        final String dataFormat = getProperty(SourceDataProperties.SOURCE_DATA_STEP, SourceDataProperties.DATA_FORMAT);
        if ("AVRO".equalsIgnoreCase(dataFormat)) {
            steps.add(SchemaRegistryProperties.SCHEMA_REGISTRY_STEP);
        }

        steps.add(SnowflakeProperties.SNOWFLAKE_STEP);
        return steps;
    }

    @Override
    public void finishUpdate() throws FlowUpdateException {
        try {
            final VersionedProcessGroup rootGroup = buildFlowDefinition();
            getInitializationContext().updateFlow(rootGroup);
        } catch (final IOException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName) {
    }

    @Override
    public void prepareUpdate() {
    }

    @Override
    public void abortUpdatePreparation(final Throwable cause) {
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
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
            KafkaConnectivityProperties.KAFKA_CONNECTION_STEP, KafkaConnectivityProperties.SECURITY_PROTOCOL);
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

    /**
     * Creates a VersionedConnection between two Process Groups using the specified port names.
     * @param sourceGroup the source Process Group
     * @param outputPortName the name of the output port in the source group
     * @param destinationGroup the destination Process Group
     * @param inputPortName the name of the input port in the destination group
     * @return the created VersionedConnection
     */
    protected VersionedConnection createConnection(final VersionedProcessGroup sourceGroup, final String outputPortName,
        final VersionedProcessGroup destinationGroup, final String inputPortName) {

        // Create the Source ConnectableComponent
        final String sourcePortId = sourceGroup.getOutputPorts().stream()
            .filter(port -> port.getName().equals(outputPortName))
            .findFirst()
            .map(VersionedComponent::getIdentifier)
            .orElseThrow(() -> new IllegalArgumentException("Output port '%s' not found in source group '%s'".formatted(outputPortName, sourceGroup.getIdentifier())));

        final ConnectableComponent connectableSource = new ConnectableComponent();
        connectableSource.setId(sourcePortId);
        connectableSource.setGroupId(sourceGroup.getIdentifier());
        connectableSource.setName(outputPortName);
        connectableSource.setType(ConnectableComponentType.OUTPUT_PORT);

        // Create the Destination ConnectableComponent
        final String destinationPortId = destinationGroup.getInputPorts().stream()
            .filter(port -> port.getName().equals(inputPortName))
            .findFirst()
            .map(VersionedComponent::getIdentifier)
            .orElseThrow(() -> new IllegalArgumentException("Input port '%s' not found in destination group '%s'".formatted(inputPortName, destinationGroup.getIdentifier())));

        final ConnectableComponent connectableDestination = new ConnectableComponent();
        connectableDestination.setId(destinationPortId);
        connectableDestination.setGroupId(destinationGroup.getIdentifier());
        connectableDestination.setName(inputPortName);
        connectableDestination.setType(ConnectableComponentType.INPUT_PORT);

        // Create the VersionedConnection
        final VersionedConnection connection = new VersionedConnection();
        connection.setSource(connectableSource);
        connection.setDestination(connectableDestination);
        connection.setIdentifier(sourceGroup.getIdentifier() + "-" + outputPortName + "-" + destinationGroup.getIdentifier() + "-" + inputPortName);
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setBackPressureObjectThreshold(10000L);
        connection.setSelectedRelationships(Set.of(""));

        return connection;
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
