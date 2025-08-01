/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.common;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorInitializationContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

public abstract class AbstractConnector implements Connector {
    private volatile ConnectorInitializationContext initializationContext;

    @Override
    public void initialize(final ConnectorInitializationContext context) {
        this.initializationContext = context;
    }

    protected final ConnectorInitializationContext getInitializationContext() {
        if (initializationContext == null) {
            throw new IllegalStateException("Connector has not been initialized");
        }

        return initializationContext;
    }

    @Override
    public void start(final Duration duration) throws FlowUpdateException, TimeoutException, InterruptedException {
        final ProcessGroupLifecycle lifecycle = getInitializationContext().getRootGroup().getLifecycle();
        final long maxTime = System.currentTimeMillis() + duration.toMillis();

        try {
            lifecycle.enableControllerServices().get(maxTime, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException | InterruptedException e) {
            throw e;
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to enable Controller Services", e);
        }

        lifecycle.startProcessors();
    }

    @Override
    public void stop(final Duration duration) throws FlowUpdateException, TimeoutException, InterruptedException {
        final long maxTime = System.currentTimeMillis() + duration.toMillis();

        final ProcessGroupLifecycle lifecycle = getInitializationContext().getRootGroup().getLifecycle();
        try {
            lifecycle.stopProcessors().get(maxTime, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException | TimeoutException e) {
            throw e;
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to stop all Processors", e);
        }

        try {
            final long remainingMillis = maxTime - System.currentTimeMillis();
            lifecycle.disableControllerServices().get(remainingMillis, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException | TimeoutException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException("Failed to disable Controller Services", e);
        }
    }

    @Override
    public void drainFlowFiles(final Duration duration) throws FlowUpdateException, TimeoutException, InterruptedException {
        stopSourceProcessors();

        final long maxTime = System.currentTimeMillis() + duration.toMillis();
        while (!isGroupDrained(getInitializationContext().getRootGroup())) {
            if (System.currentTimeMillis() > maxTime) {
                final QueueSize queueSize = getInitializationContext().getRootGroup().getQueueSize();
                if (queueSize.getObjectCount() == 0) {
                    return;
                }

                throw new TimeoutException("Timed out waiting for all FlowFiles to drain with [%s] FlowFiles ([%s] bytes) remaining in the queue".formatted(
                        queueSize.getObjectCount(), queueSize.getByteCount()));
            }

            ensureDrainage();

            Thread.sleep(1000);
        }
    }

    @Override
    public List<ValidationResult> validate() {
        final List<ValidationResult> validationResults = new ArrayList<>();
        validate(getInitializationContext().getRootGroup(), validationResults);
        return validationResults;
    }

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
     * Ensure that if there are any components that are blocking the flow from draining,
     * such as those that wait for some threshold to be reached before processing,
     * that those components are triggered to perform their tasks pre-emptively.
     */
    protected void ensureDrainage() {
    }

    protected boolean isGroupDrained(final ProcessGroupFacade group) {
        return group.getQueueSize().getObjectCount() == 0;
    }

    protected void stopSourceProcessors() throws InterruptedException, FlowUpdateException {
        final List<ProcessorFacade> sourceProcessors = getSourceProcessors();

        final List<Future<Void>> stopFutures = new ArrayList<>();
        for (final ProcessorFacade sourceProcessor : sourceProcessors) {
            stopFutures.add(sourceProcessor.getLifecycle().stop());
        }

        final CompletableFuture<Void> allStopped = CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
        try {
            allStopped.get(5, TimeUnit.MINUTES);
        } catch (final InterruptedException ie) {
            throw ie;
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to stop all Source Processors", e);
        }
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
}
