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
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.List;

public interface Connector {

    /**
     * Initializes the Connector instance, providing it the necessary context that it needs to operate.
     * @param context the context for initialization
     */
    void initialize(ConnectorInitializationContext context);

    /**
     * Stops the Connector instance.
     * @throws FlowUpdateException if there is an error stopping the Connector
     */
    void stop() throws FlowUpdateException;

    /**
     * Starts the Connector instance.
     * @throws FlowUpdateException if there is an error starting the Connector
     */
    void start() throws FlowUpdateException;

    /**
     * Drains all FlowFiles from the Connector instance. This is required in order to ensure that the
     * flow definition is able to be safely updated from one version to another.
     * @throws FlowUpdateException if there is an error draining the FlowFiles
     */
    void drainFlowFiles() throws FlowUpdateException;

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
     * Provides the definition of the flow that can be used to create a new instance of the Connector.
     *
     * @return the flow definition
     */
    VersionedProcessGroup getFlowDefinition();

    /**
     * Provides the FlowMigration instance that is responsible for migrating between different versions of the flow.
     * @return the FlowMigration instance
     */
    FlowMigration getFlowMigration();

    /**
     * Provides the Assets that are necessary for the Connector to operate.
     */
    List<ConnectorAsset> getAssets();
}
