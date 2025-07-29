/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.ProcessGroupFacade;

public interface ConnectorInitializationContext {

    String getIdentifier();

    String getName();

    ProcessGroupFacade getRootGroup();

}