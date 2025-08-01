/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ConnectorPropertyGroup {
    private final String name;
    private final String description;
    private final List<ConnectorPropertyDescriptor> propertyDescriptors;

    private ConnectorPropertyGroup(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.propertyDescriptors = Collections.unmodifiableList(builder.propertyDescriptors);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<ConnectorPropertyDescriptor> getPropertyDescriptors() {
        return propertyDescriptors;
    }


    public static final class Builder {
        private String name;
        private String description;
        private List<ConnectorPropertyDescriptor> propertyDescriptors = Collections.emptyList();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder propertyDescriptors(final List<ConnectorPropertyDescriptor> propertyDescriptors) {
            this.propertyDescriptors = new ArrayList<>(propertyDescriptors);
            return this;
        }

        public ConnectorPropertyGroup build() {
            return new ConnectorPropertyGroup(this);
        }
    }
}
