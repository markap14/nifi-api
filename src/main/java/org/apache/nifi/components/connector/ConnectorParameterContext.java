/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.util.Map;

public interface ConnectorParameterContext {

    Map<ConnectorParameter, String> getParameters();

}
