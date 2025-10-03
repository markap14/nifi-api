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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.flow.VersionedConnection;
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
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException(ie);
        }

        try {
            startNonSourceProcessors();
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException(ie);
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
            throw new FlowUpdateException("Failed to stop all source Processors", e);
        }
    }

    protected void startNonSourceProcessors() throws InterruptedException, FlowUpdateException {
        final List<ProcessorFacade> nonSourceProcessors = getNonSourceProcessors();

        final List<CompletableFuture<Void>> startFutures = new ArrayList<>();
        for (final ProcessorFacade nonSourceProcessor : nonSourceProcessors) {
            final Future<Void> startFuture = nonSourceProcessor.getLifecycle().start();
            startFutures.add(toCompletableFuture(startFuture));
        }

        final CompletableFuture<Void> allStarted = CompletableFuture.allOf(startFutures.toArray(new CompletableFuture[0]));
        try {
            allStarted.get();
        } catch (final InterruptedException ie) {
            throw ie;
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to start all non-source Processors", e);
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
        final Set<String> nonSourceIds = getNonSourceProcessorIds(group);

        return findProcessors(group, processor -> !nonSourceIds.contains(processor.getDefinition().getIdentifier()));
    }

    protected List<ProcessorFacade> getNonSourceProcessors() {
        final ProcessGroupFacade group = getInitializationContext().getRootGroup();
        final Set<String> nonSourceIds = getNonSourceProcessorIds(group);

        return findProcessors(group, processor -> nonSourceIds.contains(processor.getDefinition().getIdentifier()));
    }

    protected Set<String> getNonSourceProcessorIds(final ProcessGroupFacade group) {
        final Set<String> destinationIds = new HashSet<>();
        forEachConnection(group, conn -> {
            final VersionedConnection definition = conn.getDefinition();
            final String sourceId = definition.getSource().getId();
            final String destinationId = definition.getDestination().getId();
            if (!Objects.equals(sourceId, destinationId)) {
                destinationIds.add(destinationId);
            }
        });

        return destinationIds;
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

    protected String getProperty(final String configurationStepName, final String propertyName) {
        final ConnectorConfigurationContext configurationContext = getInitializationContext().getConfigurationContext();
        if (configurationContext == null) {
            return null;
        }

        return configurationContext.getProperty(configurationStepName, propertyName);
    }

    protected String getProperty(final ConfigurationStep configurationStep, final ConnectorPropertyDescriptor propertyDescriptor) {
        final ConnectorConfigurationContext configurationContext = getInitializationContext().getConfigurationContext();
        if (configurationContext == null) {
            return propertyDescriptor.getDefaultValue();
        }

        return configurationContext.getProperty(configurationStep, propertyDescriptor);
    }
}
