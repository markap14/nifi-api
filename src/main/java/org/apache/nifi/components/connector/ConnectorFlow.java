/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.flow.VersionedProcessGroup;

public interface ConnectorFlow {

    VersionedProcessGroup getRootGroup();

    ConnectorParameterContext getParameterContext();

}
