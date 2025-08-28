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

import java.util.List;

/**
 * <p>
 *     A Connector is a component that encapsulates and manages a NiFi flow, in such a way that the flow
 *     can be treated as a single component. The Connector is responsible for managing the lifecycle of the flow,
 *     including starting and stopping the flow, as well as validating that the flow is correctly configured.
 *     The Connector exposes a single holistic configuration that is encapsulates the configuration of the
 *     sources, sinks, transformations, routing logic, and any other components that make up the flow.
 * </p>
 *
 * <p>
 *     Importantly, a Connector represents a higher-level abstraction and is capable of manipulating the associated
 *     dataflow, including adding, removing, and configuring components within the flow. This allows a single entity to
 *     be provided such that configuring properties can result in a flow being dynamically reconfigured (e.g., using a
 *     different Controller Service implementation).
 * </p>
 *
 * <b>Implementation Note:</b> This API is currently experimental, as it is under very active development. As such,
 * it is subject to change without notice between minor releases.
 */
public interface Connector {

    /**
     * Initializes the Connector instance, providing it the necessary context that it needs to operate.
     * @param context the context for initialization
     */
    void initialize(ConnectorInitializationContext context);

    /**
     * Starts the Connector instance.
     * @throws FlowUpdateException if there is an error starting the Connector
     */
    void start() throws FlowUpdateException;

    /**
     * Stops the Connector instance.
     * @throws FlowUpdateException if there is an error stopping the Connector
     */
    void stop() throws FlowUpdateException;

    /**
     * Validates that the Connector is valid according to its current configuration. Validity of a Connector may be
     * defined simply as the all components being valid, or it may encompass more complex validation logic, such
     * as ensuring that a Source Processor is able to connect to a remote system, or that a Sink Processor
     * is able to write to a remote system.
     *
     * @return a list of ValidationResults, each of which may indicate a check that was performed and any associated explanations
     * as to why the Connector is valid or invalid.
     */
    List<ValidationResult> validate();

    /**
     * Expose the Property Descriptors that are expected to be configurable through the Custom UI.
     * @return a list of names for all property groups that are exposed by this Connector
     */
    List<String> getPropertyGroupNames();

    /**
     * Returns the Property Group that corresponds to the given group name. The group name is expected to be in the list of names provided
     * by {@link #getPropertyGroupNames()}.
     *
     * @param groupName the name of the group
     * @return the Property Group with the given name
     * @throws IllegalArgumentException if the group name does not exist
     */
    ConnectorPropertyGroup getPropertyGroup(String groupName);

    /**
     * The `onConfigured` method is called after the Connector has been (re)configured and is responsible for
     * performing any necessary actions to ensure that the flow is configured according to the Connector's configuration.
     *
     * @throws FlowUpdateException if the Connector fails to update the flow according to its configuration
     */
    void onConfigured() throws FlowUpdateException;

}
