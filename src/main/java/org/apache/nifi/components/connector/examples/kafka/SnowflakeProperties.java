/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.ConnectorPropertySubGroup;

import java.util.List;

public class SnowflakeProperties {
    static final ConnectorPropertyDescriptor DATABASE = new ConnectorPropertyDescriptor.Builder()
        .name("Database")
        .description("The Snowflake database to connect to")
        .required(true)
        .build();

    static final ConnectorPropertyDescriptor SCHEMA = new ConnectorPropertyDescriptor.Builder()
        .name("Schema")
        .description("The Snowflake schema to use")
        .required(true)
        .build();

    static final ConnectorPropertyDescriptor WAREHOUSE = new ConnectorPropertyDescriptor.Builder()
        .name("Warehouse")
        .description("The Snowflake warehouse to use for processing")
        .required(true)
        .build();

    static List<ConnectorPropertyDescriptor> snowflakeProperties = List.of(
        DATABASE,
        SCHEMA,
        WAREHOUSE
    );

    static final ConnectorPropertySubGroup SNOWFLAKE_PROPERTY_SUB_GROUP = new ConnectorPropertySubGroup.Builder()
        .name("Destination")
        .description("Properties for configuring the Snowflake destination")
        .properties(snowflakeProperties)
        .build();

    static final ConnectorPropertyGroup SNOWFLAKE_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Snowflake Configuration")
        .description("Properties for connecting to Snowflake")
        .subGroups(List.of(SNOWFLAKE_PROPERTY_SUB_GROUP))
        .build();

}
