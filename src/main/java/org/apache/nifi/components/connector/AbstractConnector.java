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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceHierarchy;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceScope;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.ProcessorState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractConnector implements Connector {
    private final Map<PropertyKey, List<AllowableValue>> cachedAllowableValues = new ConcurrentHashMap<>();

    private volatile ConnectorInitializationContext initializationContext;
    private volatile ComponentLog logger;

    protected abstract void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException;


    @Override
    public final void initialize(final ConnectorInitializationContext context, final FlowContext activeFlowContext) {
        this.initializationContext = context;
        this.logger = context.getLogger();

        try {
            init(activeFlowContext);
        } catch (final FlowUpdateException e) {
            throw new RuntimeException("Failed to initialize Connector", e);
        }
    }

    /**
     * No-op method for subclasses to override to perform any initialization logic
     *
     * @param activeFlowContext the flow context that represents the active flow
     * @throws FlowUpdateException if there is an error during initialization
     */
    protected void init(final FlowContext activeFlowContext) throws FlowUpdateException {
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
    public void start(final FlowContext context) throws FlowUpdateException {
        final ProcessGroupLifecycle lifecycle = context.getRootGroup().getLifecycle();

        try {
            lifecycle.enableControllerServices(ControllerServiceReferenceScope.INCLUDE_REFERENCED_SERVICES_ONLY, ControllerServiceReferenceHierarchy.INCLUDE_CHILD_GROUPS).get();
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to enable Controller Services", e);
        }

        lifecycle.startProcessors();
    }

    @Override
    public void stop(final FlowContext context) throws FlowUpdateException {
        final ProcessGroupFacade rootGroup = context.getRootGroup();
        final ProcessGroupLifecycle lifecycle = rootGroup.getLifecycle();
        try {
            lifecycle.stopProcessors().get(1, TimeUnit.MINUTES);
        } catch (final TimeoutException timeoutException) {
            final List<ProcessorFacade> running = findProcessors(rootGroup, processor ->
                processor.getLifecycle().getState() != ProcessorState.STOPPED && processor.getLifecycle().getState() != ProcessorState.DISABLED);

            if (!running.isEmpty()) {
                getLogger().warn("After waiting 60 seconds for all Processors to stop, {} are still running. Terminating now.", running.size());
                running.forEach(processor -> processor.getLifecycle().terminate());
            }
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to stop all Processors", e);
        }

        try {
            lifecycle.disableControllerServices(ControllerServiceReferenceHierarchy.INCLUDE_CHILD_GROUPS).get(1, TimeUnit.MINUTES);
        } catch (final Exception e) {
            throw new FlowUpdateException("Failed to disable Controller Services", e);
        }
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        stop(activeContext);
    }

    /**
     * Drains all FlowFiles from the Connector instance.
     *
     * @param flowContext the FlowContext to use for drainage
     * @throws FlowUpdateException if there is an error draining the FlowFiles
     */
    protected void drainFlowFiles(final FlowContext flowContext) throws FlowUpdateException {
        try {
            stopSourceProcessors(flowContext);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException(ie);
        }

        try {
            startNonSourceProcessors(flowContext);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException(ie);
        }

        try {
            ensureDrainageUnblocked();
        } catch (final InvocationFailedException e) {
            throw new FlowUpdateException(e);
        }

        while (!isGroupDrained(flowContext.getRootGroup())) {
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
    public List<ValidationResult> validate(final FlowContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        validate(context, context.getRootGroup(), validationResults);
        return validationResults;
    }

    private void validate(final FlowContext context, final ProcessGroupFacade group, final List<ValidationResult> validationResults) {
        final List<ValidationResult> connectorPropertiesResults = validate(context, context.getConfigurationContext());
        if (!connectorPropertiesResults.isEmpty()) {
            connectorPropertiesResults.stream()
                .filter(result -> !result.isValid())
                .forEach(validationResults::add);

            // If any invalid results on the connector configuration itself, do not proceed with further validation of components
            if (!validationResults.isEmpty()) {
                return;
            }
        }

        for (final ProcessorFacade processor : group.getProcessors()) {
            final List<ValidationResult> processorResults = processor.validate();
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

        final Set<ControllerServiceFacade> referencedServices = group.getControllerServices(
            ControllerServiceReferenceScope.INCLUDE_REFERENCED_SERVICES_ONLY,
            ControllerServiceReferenceHierarchy.DIRECT_SERVICES_ONLY);

        for (final ControllerServiceFacade service : referencedServices) {
            final List<ValidationResult> serviceResults = service.validate();
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
            validate(context, childGroup, validationResults);
        }
    }

    protected boolean isGroupDrained(final ProcessGroupFacade group) {
        return group.getQueueSize().getObjectCount() == 0;
    }

    protected void stopSourceProcessors(final FlowContext context) throws InterruptedException, FlowUpdateException {
        final List<ProcessorFacade> sourceProcessors = getSourceProcessors(context.getRootGroup());

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

    protected void startNonSourceProcessors(final FlowContext flowContext) throws InterruptedException, FlowUpdateException {
        final List<ProcessorFacade> nonSourceProcessors = getNonSourceProcessors(flowContext.getRootGroup());

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

    protected List<ProcessorFacade> getSourceProcessors(final ProcessGroupFacade group) {
        final Set<String> nonSourceIds = getNonSourceProcessorIds(group);

        return findProcessors(group, processor -> !nonSourceIds.contains(processor.getDefinition().getIdentifier()));
    }

    protected List<ProcessorFacade> getNonSourceProcessors(final ProcessGroupFacade group) {
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


    @Override
    public List<ValidationResult> validate(final FlowContext workingContext, final ConnectorConfigurationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final List<ConfigurationStep> configurationSteps = getConfigurationSteps(workingContext);

        for (final ConfigurationStep configurationStep : configurationSteps) {
            final List<ConnectorPropertyGroup> propertyGroups = configurationStep.getPropertyGroups();

            for (final ConnectorPropertyGroup propertyGroup : propertyGroups) {
                final List<ConnectorPropertyDescriptor> descriptors = propertyGroup.getProperties();
                final Map<String, ConnectorPropertyDescriptor> descriptorMap = descriptors.stream()
                    .collect(Collectors.toMap(ConnectorPropertyDescriptor::getName, Function.identity()));

                final Function<String, ConnectorPropertyValue> propertyValueLookup = name -> context.getProperty(configurationStep.getName(), name);

                for (final ConnectorPropertyDescriptor descriptor : descriptors) {
                    final boolean dependencySatisfied = isDependencySatisfied(descriptor, descriptorMap::get, propertyValueLookup);

                    // If the property descriptor's dependency is not satisfied, the property does not need to be considered, as it's not relevant to the
                    if (!dependencySatisfied) {
                        continue;
                    }

                    final ConnectorPropertyValue propertyValue = context.getProperty(configurationStep.getName(), descriptor.getName());
                    if (propertyValue == null || !propertyValue.isSet()) {
                        if (descriptor.isRequired()) {
                            final ValidationResult invalidResult = new ValidationResult.Builder()
                                .valid(false)
                                .input(null)
                                .subject(descriptor.getName())
                                .explanation(descriptor.getName() + " is required")
                                .build();
                            results.add(invalidResult);
                        }

                        continue;
                    }

                    final ValidationResult result = descriptor.validate(propertyValue.getValue());
                    if (!result.isValid()) {
                        results.add(result);
                    }

                    final List<DescribedValue> allowableValues = descriptor.getAllowableValues();
                    if (!isValueAllowed(propertyValue.getValue(), allowableValues)) {
                        final ValidationResult invalidResult = new ValidationResult.Builder()
                            .valid(false)
                            .input(propertyValue.getValue())
                            .subject(descriptor.getName())
                            .explanation("Value is not one of the allowable values")
                            .build();

                        results.add(invalidResult);
                    }

                    final boolean allowableValuesFetchable = descriptor.isAllowableValuesFetchable();
                    if (allowableValuesFetchable) {
                        final PropertyKey key = new PropertyKey(configurationStep.getName(), propertyGroup.getName(), descriptor.getName());
                        List<AllowableValue> fetchedAllowableValues = cachedAllowableValues.get(key);
                        if (fetchedAllowableValues == null) {
                            try {
                                fetchedAllowableValues = fetchAllowableValues(configurationStep.getName(), propertyGroup.getName(), descriptor.getName(), workingContext);
                            } catch (final Exception e) {
                                getLogger().error("Failed to validate property {} due to failure fetching allowable values", descriptor.getName(), e);

                                final ValidationResult invalidResult = new ValidationResult.Builder()
                                    .valid(false)
                                    .input(propertyValue.getValue())
                                    .subject(descriptor.getName())
                                    .explanation("Failed to fetch allowable values: " + e.getMessage())
                                    .build();

                                results.add(invalidResult);
                                continue;
                            }
                        }

                        if (!isValueAllowed(propertyValue.getValue(), fetchedAllowableValues)) {
                            final ValidationResult invalidResult = new ValidationResult.Builder()
                                .valid(false)
                                .input(propertyValue.getValue())
                                .subject(descriptor.getName())
                                .explanation("Value is not one of the allowable values")
                                .build();

                            results.add(invalidResult);
                        }
                    }
                }
            }
        }

        // only run customValidate if regular validation is successful. This allows Processor developers to not have to check
        // if values are null or invalid so that they can focus only on the interaction between the properties, etc.
        if (results.isEmpty()) {
            final Collection<ValidationResult> customResults = customValidate(context);
            if (customResults != null) {
                for (final ValidationResult result : customResults) {
                    if (!result.isValid()) {
                        results.add(result);
                    }
                }
            }
        }

        return results;
    }

    private boolean isValueAllowed(final String value, final List<? extends DescribedValue> allowableValues) {
        if (allowableValues == null || allowableValues.isEmpty()) {
            // If no allowable values are explicitly specified, consider all values to be allowable
            return true;
        }
        if (value == null) {
            return false;
        }

        for (final DescribedValue describedValue : allowableValues) {
            if (value.equalsIgnoreCase(describedValue.getValue())) {
                return true;
            }
        }

        return false;
    }

    private boolean isDependencySatisfied(final ConnectorPropertyDescriptor propertyDescriptor, final Function<String, ConnectorPropertyDescriptor> propertyDescriptorLookup,
            final Function<String, ConnectorPropertyValue> propertyValueLookup) {

        return isDependencySatisfied(propertyDescriptor, propertyDescriptorLookup, propertyValueLookup, new HashSet<>());
    }

    private boolean isDependencySatisfied(final ConnectorPropertyDescriptor propertyDescriptor, final Function<String, ConnectorPropertyDescriptor> propertyDescriptorLookup,
            final Function<String, ConnectorPropertyValue> propertyValueLookup, final Set<String> propertiesSeen) {

        final Set<ConnectorPropertyDependency> dependencies = propertyDescriptor.getDependencies();
        if (dependencies.isEmpty()) {
            return true;
        }

        final boolean added = propertiesSeen.add(propertyDescriptor.getName());
        if (!added) {
            return false;
        }

        try {
            for (final ConnectorPropertyDependency dependency : dependencies) {
                final String dependencyName = dependency.getPropertyName();

                // Check if the property being depended upon has its dependencies satisfied.
                final ConnectorPropertyDescriptor dependencyDescriptor = propertyDescriptorLookup.apply(dependencyName);
                if (dependencyDescriptor == null) {
                    return false;
                }

                final ConnectorPropertyValue propertyValue = propertyValueLookup.apply(dependencyDescriptor.getName());
                final String dependencyValue = propertyValue == null ? dependencyDescriptor.getDefaultValue() : propertyValue.getValue();
                if (dependencyValue == null) {
                    return false;
                }

                final boolean transitiveDependencySatisfied = isDependencySatisfied(dependencyDescriptor, propertyDescriptorLookup, propertyValueLookup, propertiesSeen);
                if (!transitiveDependencySatisfied) {
                    return false;
                }

                // Check if the property being depended upon is set to one of the values that satisfies this dependency.
                // If the dependency has no dependent values, then any non-null value satisfies the dependency.
                // The value is already known to be non-null due to the check above.
                final Set<String> dependentValues = dependency.getDependentValues();
                if (dependentValues != null && !dependentValues.contains(dependencyValue)) {
                    return false;
                }
            }

            return true;
        } finally {
            propertiesSeen.remove(propertyDescriptor.getName());
        }
    }

    @Override
    public final void onConfigurationStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        onStepConfigured(stepName, workingContext);
        cachedAllowableValues.clear();
    }

    @Override
    public void abortUpdatePreparation(final FlowContext workingContext, final Throwable throwable) {
    }

    @Override
    public final List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName, final FlowContext flowContext) {
        final List<AllowableValue> allowableValues = fetchAllAllowableValues(stepName, groupName, propertyName, flowContext);
        final PropertyKey key = new PropertyKey(stepName, groupName, propertyName);
        cachedAllowableValues.put(key, allowableValues);
        return allowableValues;
    }

    protected List<AllowableValue> fetchAllAllowableValues(final String stepName, final String groupName, final String propertyName, final FlowContext flowContext) {
        throw new UnsupportedOperationException("Property %s of Property Group %s in Configuration Step %s does not support fetching Allowable Values.".formatted(propertyName, groupName, stepName));
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName, final FlowContext flowContext, final String filter) {
        final List<AllowableValue> allowableValues = fetchAllowableValues(stepName, groupName, propertyName, flowContext);
        if (filter == null || filter.isEmpty()) {
            return allowableValues;
        } else {
            return allowableValues.stream()
                .filter(value -> value.getValue().toLowerCase().contains(filter.toLowerCase()) || value.getValue().toUpperCase().contains(filter.toUpperCase()))
                .toList();
        }
    }


    /**
     * No-op implementation that allows concrete subclasses to perform validation of property configuration
     *
     * @param context the context that should be used for validation
     * @return a collection of validation results indicating any problems with the configuration.
     */
    protected Collection<ValidationResult> customValidate(final ConnectorConfigurationContext context) {
        return Collections.emptyList();
    }

    protected record PropertyKey(String stepName, String groupName, String propertyName) {
    }
}
