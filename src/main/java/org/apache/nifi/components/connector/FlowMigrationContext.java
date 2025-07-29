/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.flow.VersionedProcessGroup;

public interface FlowMigrationContext {

    /**
     * Provides the definition of the flow as it exists before migration.
     * @return the flow definition before migration
     */
    VersionedProcessGroup getFlowBeforeMigration();

    /**
     * Provides the definition of the flow as it exists or will exist after migration.
     * @return the flow definition after migration
     */
    VersionedProcessGroup getFlowAfterMigration();

}
