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
