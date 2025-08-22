/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

public interface ConnectorConfigurationContext {

    String getProperty(String propertyGroupName, String propertyName);

    String getProperty(ConnectorPropertyGroup propertyGroup, ConnectorPropertyDescriptor propertyDescriptor);

}
