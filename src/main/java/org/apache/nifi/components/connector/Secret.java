/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

public interface Secret {

    String getGroupName();

    String getName();

    String getValue();

}
