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
    private final List<ConnectorPropertySubGroup> subGroups;

    private ConnectorPropertyGroup(final Builder builder) {
        this.name = builder.name;
        this.description = builder.description;
        this.subGroups = Collections.unmodifiableList(builder.subGroups);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<ConnectorPropertySubGroup> getSubGroups() {
        return subGroups;
    }


    public static final class Builder {
        private String name;
        private String description;
        private List<ConnectorPropertySubGroup> subGroups = Collections.emptyList();

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder subGroups(final List<ConnectorPropertySubGroup> subGroups) {
            this.subGroups = new ArrayList<>(subGroups);
            return this;
        }

        public ConnectorPropertyGroup build() {
            return new ConnectorPropertyGroup(this);
        }
    }
}
