/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class ConnectorPropertyDescriptor {
    private final String name;
    private final String description;
    private final String defaultValue;
    private final boolean required;
    private final PropertyType type;
    private final List<DescribedValue> allowableValues;
    private final List<Validator> validators;

    private ConnectorPropertyDescriptor(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.required = builder.required;
        this.type = builder.type;
        this.allowableValues = builder.allowableValues == null ? null : Collections.unmodifiableList(builder.allowableValues);
        this.validators = List.copyOf(builder.validators);
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

    public List<DescribedValue> getAllowableValues() {
        return allowableValues;
    }

    public List<Validator> getValidators() {
        return validators;
    }


    public static final class Builder {
        private String name;
        private String description;
        private String defaultValue = null;
        private boolean required = false;
        private PropertyType type = PropertyType.STRING;
        private List<DescribedValue> allowableValues = null;
        private final List<Validator> validators = new ArrayList<>();
        private final Set<ConnectorPropertyDependency> dependencies = new HashSet<>();

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

        public Builder defaultValue(final DescribedValue defaultValue) {
            return defaultValue(defaultValue == null ? null : defaultValue.getValue());
        }

        public Builder required(final boolean required) {
            this.required = required;
            return this;
        }

        public Builder type(final PropertyType type) {
            this.type = type;
            return this;
        }

        public Builder allowableValues(final DescribedValue... values) {
            this.allowableValues = Arrays.stream(values)
                .map(Builder::describedValue)
                .toList();

            return this;
        }

        public <E extends Enum<E>> Builder allowableValues(final E[] values) {
            if (values == null || values.length == 0) {
                this.allowableValues = null;
            } else {
                this.allowableValues = Arrays.stream(values)
                    .map(enumValue -> enumValue instanceof DescribedValue describedValue
                        ? AllowableValue.fromDescribedValue(describedValue) : new AllowableValue(enumValue.name()))
                    .map(av -> (DescribedValue) av)
                    .toList();
            }

            return this;
        }

        public <E extends Enum<E>> Builder allowableValues(final EnumSet<E> enumValues) {
            if (enumValues == null || enumValues.isEmpty()) {
                this.allowableValues = null;
            } else {
                this.allowableValues = enumValues.stream()
                    .map(enumValue -> enumValue instanceof DescribedValue describedValue
                        ? AllowableValue.fromDescribedValue(describedValue) : new AllowableValue(enumValue.name()))
                    .map(av -> (DescribedValue) av)
                    .toList();
            }

            return this;
        }

        public Builder allowableValues(final String... allowableValues) {
            if (allowableValues == null || allowableValues.length == 0) {
                this.allowableValues = null;
            } else {
                this.allowableValues = Arrays.stream(allowableValues)
                    .map(Builder::describedValue)
                    .toList();
            }

            return this;
        }

        /**
         * Adds a validator for this property
         *
         * @param validator the validator to add
         * @return this Builder for method chaining
         */
        public Builder addValidator(final Validator validator) {
            if (validator != null) {
                this.validators.add(validator);
            }
            return this;
        }

        /**
         * Removes all validators for this property
         *
         * @return this Builder for method chaining
         */
        public Builder clearValidators() {
            this.validators.clear();
            return this;
        }

        /**
         * Sets the validators for this property, replacing any previously added validators
         *
         * @param validators the validators to set
         * @return this Builder for method chaining
         */
        public Builder validators(final Validator... validators) {
            this.validators.clear();

            if (validators != null) {
                for (final Validator validator : validators) {
                    if (validator != null) {
                        this.validators.add(validator);
                    }
                }
            }

            return this;
        }

        public Builder dependsOn(final ConnectorPropertyDescriptor descriptor, final List<DescribedValue> dependentValues) {
            if (dependentValues == null || dependentValues.isEmpty()) {
                dependencies.add(new ConnectorPropertyDependency(descriptor.getName()));
            } else {
                final Set<String> dependentValueSet = dependentValues.stream()
                    .map(DescribedValue::getValue)
                    .collect(Collectors.toSet());

                dependencies.add(new ConnectorPropertyDependency(descriptor.getName(), dependentValueSet));
            }

            return this;
        }

        public Builder dependsOn(final ConnectorPropertyDescriptor descriptor, final DescribedValue... dependentValues) {
            return dependsOn(descriptor, Arrays.asList(dependentValues));
        }

        public Builder dependsOn(final ConnectorPropertyDescriptor descriptor, final String... dependentValues) {
            final List<DescribedValue> describedValues = Arrays.stream(dependentValues)
                    .map(Builder::describedValue)
                    .toList();

            return dependsOn(descriptor, describedValues);
        }

        private static DescribedValue describedValue(final String value) {
            if (value == null) {
                return null;
            }

            // Otherwise, return a generic DescribedValue with no display name or description
            return new AllowableValue(value);
        }

        private static DescribedValue describedValue(final DescribedValue describedValue) {
            if (describedValue == null) {
                return null;
            }

            return new AllowableValue(describedValue.getValue(), describedValue.getDisplayName(), describedValue.getDescription());
        }

        public ConnectorPropertyDescriptor build() {
            return new ConnectorPropertyDescriptor(this);
        }
    }

}
