/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.flow.VersionedProcessGroup;

public interface FlowMigration {

    /**
     * Provides a new definition of the flow, based on the current definition of the flow.
     * This provides the Connector with the ability to migrate the flow definition to a new version.
     *
     * @param flowDefinition the current definition of the flow
     * @return the migrated flow definition
     */
    VersionedProcessGroup migrateFlowDefinition(VersionedProcessGroup flowDefinition);

    /**
     * Performs any necessary actions before a flow is migrated to a new version of the flow.
     */
    void beforeFlowMigration(FlowMigrationContext context);

    /**
     * Performs any necessary cleanup or finalization after a flow has been migrated to a new version.
     */
    void afterFlowMigration(FlowMigrationContext context);

}
