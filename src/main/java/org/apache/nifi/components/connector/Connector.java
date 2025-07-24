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
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.List;

public interface Connector {

    /**
     * Stops the Connector instance.
     * @param rootGroup the ProcessGroupFacade that represents the root group of the flow
     * @throws FlowUpdateException if there is an error stopping the Connector
     */
    void stop(ProcessGroupFacade rootGroup) throws FlowUpdateException;

    /**
     * Starts the Connector instance.
     * @param rootGroup the ProcessGroupFacade that represents the root group of the flow
     * @throws FlowUpdateException if there is an error starting the Connector
     */
    void start(ProcessGroupFacade rootGroup) throws FlowUpdateException;

    /**
     * Validates that the Connector is valid according to its configuration. Validity of a Connector may be
     * defined simply as the all components being valid, or it may encompass more complex validation logic, such
     * as ensuring that a Source Processor is able to connect to a remote system, or that a Sink Processor
     * is able to write to a remote system.
     *
     * @param rootGroup the ProcessGroupFacade that represents the root group of the flow
     * @return a list of ValidationResults, each of which may indicate a check that was performed and any associated explanations
     * as to why the Connector is valid or invalid.
     */
    List<ValidationResult> validate(ProcessGroupFacade rootGroup);

    /**
     * Provides the definition of the flow that can be used to create a new instance of the Connector.
     *
     * @return the flow definition
     */
    VersionedProcessGroup getFlowDefinition();

    /**
     * Provides a new definition of the flow, based on the current definition of the flow.
     * This provides the Connector with the ability to migrate the flow definition to a new version.
     *
     * @param flowDefinition the current definition of the flow
     * @return the migrated flow definition
     */
    VersionedProcessGroup migrateFlowDefinition(VersionedProcessGroup flowDefinition);
}
