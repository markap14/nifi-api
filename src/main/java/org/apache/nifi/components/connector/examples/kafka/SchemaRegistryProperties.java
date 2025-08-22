/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.ConnectorPropertySubGroup;

import java.util.List;

public class SchemaRegistryProperties {
    private static List<ConnectorPropertyDescriptor> schemaRegistryProperties = List.of(
    );

    static final ConnectorPropertySubGroup SCHEMA_REGISTRY_SUB_GROUP = new ConnectorPropertySubGroup.Builder()
        .description("Properties for configuring the schema registry")
        .properties(schemaRegistryProperties)
        .build();

    static final ConnectorPropertyGroup SCHEMA_REGISTRY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Schema Registry")
        .description("Properties for connecting to Snowflake")
        .subGroups(List.of(SCHEMA_REGISTRY_SUB_GROUP))
        .build();

}
