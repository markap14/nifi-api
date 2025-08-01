/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

public interface ConnectorParameter {

    String getName();

    String getDescription();

    String getDefaultValue();

    boolean isSensitive();

}
