/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class ConnectorPropertyDescriptor {
    private final String name;
    private final String description;
    private final String defaultValue;
    private final boolean required;
    private final PropertyType type;
    private final boolean sensitive;
    private final List<String> allowableValues;

    private ConnectorPropertyDescriptor(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.required = builder.required;
        this.type = builder.type;
        this.sensitive = builder.sensitive;
        this.allowableValues = builder.allowableValues == null ? null : Collections.unmodifiableList(builder.allowableValues);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    public PropertyType getType() {
        return type;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public List<String> getAllowableValues() {
        return allowableValues;
    }

    public static final class Builder {
        private String name;
        private String description;
        private String defaultValue = null;
        private boolean required = false;
        private PropertyType type = PropertyType.STRING;
        private boolean sensitive = false;
        private List<String> allowableValues = null;

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder defaultValue(final String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder required(final boolean required) {
            this.required = required;
            return this;
        }

        public Builder type(final PropertyType type) {
            this.type = type;
            return this;
        }

        public Builder sensitive(final boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        public Builder allowableValues(final List<String> allowableValues) {
            this.allowableValues = allowableValues == null ? null : new ArrayList<>(allowableValues);
            return this;
        }

        public Builder allowableValues(final String... allowableValues) {
            if (allowableValues == null || allowableValues.length == 0) {
                this.allowableValues = null;
            } else {
                this.allowableValues = Arrays.asList(allowableValues);
            }
            return this;
        }

        public ConnectorPropertyDescriptor build() {
            return new ConnectorPropertyDescriptor(this);
        }
    }

}
