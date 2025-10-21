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

package org.apache.nifi.components.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ConfigurationStep {
    private final String name;
    private final String description;
    private final List<ConnectorPropertyGroup> propertyGroups;

    private ConfigurationStep(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.propertyGroups = Collections.unmodifiableList(builder.propertyGroups);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<ConnectorPropertyGroup> getPropertyGroups() {
        return propertyGroups;
    }

    public ConfigurationStep withAllowableValues(final String groupName, final String propertyName, final List<String> allowableValues) {
        final List<ConnectorPropertyGroup> updatedGroups = new ArrayList<>();
        for (final ConnectorPropertyGroup group : propertyGroups) {
            if (group.getName().equals(groupName)) {
                final List<ConnectorPropertyDescriptor> properties = group.getProperties();
                final List<ConnectorPropertyDescriptor> enrichedProperties = new ArrayList<>();
                for (final ConnectorPropertyDescriptor property : properties) {
                    if (property.getName().equals(propertyName)) {
                        enrichedProperties.add(new ConnectorPropertyDescriptor.Builder()
                            .from(property)
                            .allowableValues(allowableValues)
                            .build());
                    } else {
                        enrichedProperties.add(property);
                    }
                }

                final ConnectorPropertyGroup updatedGroup = new ConnectorPropertyGroup.Builder()
                    .name(group.getName())
                    .description(group.getDescription())
                    .properties(enrichedProperties)
                    .build();

                updatedGroups.add(updatedGroup);
            } else {
                updatedGroups.add(group);
            }
        }

        return new Builder()
            .name(name)
            .description(description)
            .propertyGroups(updatedGroups)
            .build();
    }

    public static final class Builder {
        private String name;
        private String description;
        private List<ConnectorPropertyGroup> propertyGroups = Collections.emptyList();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder propertyGroups(final List<ConnectorPropertyGroup> propertyGroups) {
            this.propertyGroups = new ArrayList<>(propertyGroups);
            return this;
        }

        public ConfigurationStep build() {
            if (name == null) {
                throw new IllegalStateException("Configuration Step's name must be provided");
            }

            return new ConfigurationStep(this);
        }
    }
}
