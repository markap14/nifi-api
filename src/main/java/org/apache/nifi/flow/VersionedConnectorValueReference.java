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

package org.apache.nifi.flow;

import java.util.Objects;

/**
 * Represents a property value reference for a Connector in a versioned flow.
 * This class is used for serialization/deserialization of connector property values
 * that may reference different types of values (literals, assets, secrets).
 */
public class VersionedConnectorValueReference {
    private String value;
    private String valueType;

    public VersionedConnectorValueReference() {
    }

    public VersionedConnectorValueReference(final String value, final String valueType) {
        this.value = value;
        this.valueType = valueType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(final String valueType) {
        this.valueType = valueType;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof VersionedConnectorValueReference other)) {
            return false;
        }
        return Objects.equals(value, other.value) && Objects.equals(valueType, other.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, valueType);
    }

    @Override
    public String toString() {
        return "VersionedConnectorValueReference[valueType=" + valueType + ", value=" + value + "]";
    }
}

