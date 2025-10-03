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
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class SourceDataProperties {

    private SourceDataProperties() {
    }

    static final ConnectorPropertyDescriptor TOPICS = new ConnectorPropertyDescriptor.Builder()
        .name("Topics")
        .description("The Kafka Topics to consume from")
        .required(true)
        .type(PropertyType.STRING_LIST)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor DATA_FORMAT = new ConnectorPropertyDescriptor.Builder()
        .name("Data Format")
        .description("The format of the data in the Kafka topics")
        .required(true)
        .allowableValues("JSON", "AVRO")
        .defaultValue("JSON")
        .build();

    static final ConnectorPropertyGroup SOURCE_DATA_GROUP = ConnectorPropertyGroup.builder()
        .name("Source Data")
        .description("Properties for configuring Kafka Topic and data format")
        .properties(List.of(TOPICS))
        .build();

    static final ConfigurationStep SOURCE_DATA_STEP = new ConfigurationStep.Builder()
        .name("Kafka Data")
        .description("Properties for configuring the source of data")
        .propertyGroups(List.of(SOURCE_DATA_GROUP))
        .build();

}
