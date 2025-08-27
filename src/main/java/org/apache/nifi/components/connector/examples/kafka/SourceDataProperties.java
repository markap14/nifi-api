/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.examples.kafka;

import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.ConnectorPropertySubGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

public class SourceDataProperties {

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

    static final ConnectorPropertySubGroup SOURCE_DATA_SUB_GROUP = ConnectorPropertySubGroup.builder()
        .name("Source Data")
        .description("Properties for configuring Kafka Topic and data format")
        .properties(List.of(TOPICS))
        .build();

    static final ConnectorPropertyGroup SOURCE_DATA_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Kafka Data")
        .description("Properties for configuring the source of data")
        .subGroups(List.of(SOURCE_DATA_SUB_GROUP))
        .build();

}
