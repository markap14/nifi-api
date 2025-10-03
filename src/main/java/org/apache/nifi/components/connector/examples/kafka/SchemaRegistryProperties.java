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

public class SchemaRegistryProperties {
    private static List<ConnectorPropertyDescriptor> schemaRegistryProperties = List.of(
    );

    private SchemaRegistryProperties() {
    }

    static final ConnectorPropertyGroup SCHEMA_REGISTRY_GROUP = new ConnectorPropertyGroup.Builder()
        .description("Properties for configuring the schema registry")
        .properties(schemaRegistryProperties)
        .build();

    static final ConfigurationStep SCHEMA_REGISTRY_STEP = new ConfigurationStep.Builder()
        .name("Schema Registry")
        .description("Properties for connecting to Snowflake")
        .propertyGroups(List.of(SCHEMA_REGISTRY_GROUP))
        .build();

}
