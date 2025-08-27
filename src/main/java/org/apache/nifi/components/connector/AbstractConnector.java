/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Predicate;

public abstract class AbstractConnector implements Connector {
    private volatile ConnectorInitializationContext initializationContext;
    private volatile ComponentLog logger;

    @Override
    public final void initialize(final ConnectorInitializationContext context) {
        this.initializationContext = context;
        this.logger = context.getLogger();

        try {
            init();
        } catch (final FlowUpdateException e) {
            throw new RuntimeException("Failed to initialize Connector", e);
        }
    }

    /**
     * No-op method for subclasses to override to perform any initialization logic
     */
    protected void init() throws FlowUpdateException {
    }

    protected final ComponentLog getLogger() {
        return logger;
    }

    protected final ConnectorInitializationContext getInitializationContext() {
        if (initializationContext == null) {
            throw new IllegalStateException("Connector has not been initialized");
        }

        return initializationContext;
    }

    @Override
    public void start() throws FlowUpdateException {
        final ProcessGroupLifecycle lifecycle = getInitializationContext().getRootGroup().getLifecycle();

        try {
            lifecycle.enableControllerServices().get();
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to enable Controller Services", e);
        }

        lifecycle.startProcessors();
    }

    @Override
    public void stop() throws FlowUpdateException {
        final ProcessGroupLifecycle lifecycle = getInitializationContext().getRootGroup().getLifecycle();
        try {
            lifecycle.stopProcessors().get();
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to stop all Processors", e);
        }

        try {
            lifecycle.disableControllerServices().get();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to disable Controller Services", e);
        }
    }


    /**
     * Drains all FlowFiles from the Connector instance.
     *
     * @throws FlowUpdateException if there is an error draining the FlowFiles
     */
    protected void drainFlowFiles() throws FlowUpdateException {
        try {
            stopSourceProcessors();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException(e);
        }

        try {
            ensureDrainageUnblocked();
        } catch (final InvocationFailedException e) {
            throw new FlowUpdateException(e);
        }

        while (!isGroupDrained(getInitializationContext().getRootGroup())) {
            try {
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlowUpdateException(e);
            }
        }
    }

    /**
     * <p>
     *     A method designed to be overridden by subclasses that need to ensure that any
     *     blockages to FlowFile drainage are removed. The default implementation is a no-op.
     *     Typical use cases include notifying Processors that block until a certain amount of data is queued up,
     *     or until certain conditions are met, that they should immediately allow data to flow through.
     * </p>
     */
    protected void ensureDrainageUnblocked() throws InvocationFailedException {
    }

    @Override
    public List<ValidationResult> validate() {
        final List<ValidationResult> validationResults = new ArrayList<>();
        validate(getInitializationContext().getRootGroup(), validationResults);
        return validationResults;
    }

    private void validate(final ProcessGroupFacade group, final List<ValidationResult> validationResults) {
        for (final ProcessorFacade processor : group.getProcessors()) {
            final Map<String, String> properties = processor.getDefinition().getProperties();
            final List<ValidationResult> processorResults = processor.validate(properties);
            for (final ValidationResult result : processorResults) {
                if (result.isValid()) {
                    continue;
                }

                validationResults.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(result.getSubject())
                    .input(result.getInput())
                    .explanation("Processor [%s] is invalid: %s".formatted(processor.getDefinition().getName(), result.getExplanation()))
                    .build());
            }
        }

        for (final ControllerServiceFacade service : group.getControllerServices()) {
            final Map<String, String> properties = service.getDefinition().getProperties();
            final List<ValidationResult> serviceResults = service.validate(properties);
            for (final ValidationResult result : serviceResults) {
                if (result.isValid()) {
                    continue;
                }

                validationResults.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(result.getSubject())
                    .input(result.getInput())
                    .explanation("Controller Service [%s] is invalid: %s".formatted(service.getDefinition().getName(), result.getExplanation()))
                    .build());
            }
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            validate(childGroup, validationResults);
        }
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

    protected boolean isGroupDrained(final ProcessGroupFacade group) {
        return group.getQueueSize().getObjectCount() == 0;
    }

    protected void stopSourceProcessors() throws InterruptedException, FlowUpdateException {
        final List<ProcessorFacade> sourceProcessors = getSourceProcessors();

        final List<CompletableFuture<Void>> stopFutures = new ArrayList<>();
        for (final ProcessorFacade sourceProcessor : sourceProcessors) {
            final Future<Void> stopFuture = sourceProcessor.getLifecycle().stop();
            stopFutures.add(toCompletableFuture(stopFuture));
        }

        final CompletableFuture<Void> allStopped = CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
        try {
            allStopped.get();
        } catch (final InterruptedException ie) {
            throw ie;
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to stop all Source Processors", e);
        }
    }

    private <T> CompletableFuture<T> toCompletableFuture(final Future<T> future) {
        if (future instanceof CompletableFuture) {
            return (CompletableFuture<T>) future;
        }

        // Wrap a non-CompletableFuture in a CompletableFuture
        return CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected List<ProcessorFacade> getSourceProcessors() {
        final ProcessGroupFacade group = getInitializationContext().getRootGroup();
        final Set<String> destinationIds = new HashSet<>();
        forEachConnection(group, conn -> {
            final VersionedConnection definition = conn.getDefinition();
            final String sourceId = definition.getSource().getId();
            final String destinationId = definition.getDestination().getId();
            if (!Objects.equals(sourceId, destinationId)) {
                destinationIds.add(destinationId);
            }
        });

        return findProcessors(group, processor -> !destinationIds.contains(processor.getDefinition().getIdentifier()));
    }

    protected List<ProcessorFacade> findProcessors(final ProcessGroupFacade group, final Predicate<ProcessorFacade> filter) {
        final List<ProcessorFacade> matching = new ArrayList<>();
        findProcessors(group, filter, matching);
        return matching;
    }

    private void findProcessors(final ProcessGroupFacade group, final Predicate<ProcessorFacade> filter, final List<ProcessorFacade> found) {
        for (final ProcessorFacade processor : group.getProcessors()) {
            if (filter.test(processor)) {
                found.add(processor);
            }
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            findProcessors(childGroup, filter, found);
        }
    }

    private void forEachConnection(final ProcessGroupFacade group, final Consumer<ConnectionFacade> connectionConsumer) {
        for (final ConnectionFacade connection : group.getConnections()) {
            connectionConsumer.accept(connection);
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            forEachConnection(childGroup, connectionConsumer);
        }
    }

    protected String getProperty(final String propertyGroupName, final String propertyName) {
        return getInitializationContext().getConfigurationContext().getProperty(propertyGroupName, propertyName);
    }

    protected String getProperty(final ConnectorPropertyGroup propertyGroup, final ConnectorPropertyDescriptor propertyDescriptor) {
        return getInitializationContext().getConfigurationContext().getProperty(propertyGroup, propertyDescriptor);
    }
}
