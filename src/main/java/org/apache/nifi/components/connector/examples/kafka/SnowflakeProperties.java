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

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;

import java.util.List;

public class SnowflakeProperties {

    private SnowflakeProperties() {
    }

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

    static final ConnectorPropertyGroup SNOWFLAKE_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Destination")
        .description("Properties for configuring the Snowflake destination")
        .properties(snowflakeProperties)
        .build();

    static final ConfigurationStep SNOWFLAKE_STEP = new ConfigurationStep.Builder()
        .name("Snowflake Configuration")
        .description("Properties for connecting to Snowflake")
        .propertyGroups(List.of(SNOWFLAKE_GROUP))
        .build();

}
