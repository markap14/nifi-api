/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.util.Set;

public final class ConnectorPropertyDependency {
    private final String propertyName;
    private final Set<String> dependentValues;

    public ConnectorPropertyDependency(final String propertyName, final Set<String> dependentValues) {
        this.propertyName = propertyName;
        this.dependentValues = Set.copyOf(dependentValues);
    }

    public ConnectorPropertyDependency(final String propertyName) {
        this.propertyName = propertyName;
        this.dependentValues = null;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Set<String> getDependentValues() {
        return dependentValues;
    }
}