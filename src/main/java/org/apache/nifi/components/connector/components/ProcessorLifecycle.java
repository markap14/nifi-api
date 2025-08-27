/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.util.Map;
import java.util.concurrent.Future;

public interface ProcessorLifecycle {

    ProcessorState getState();

    int getActiveThreadCount();

    void terminate();

    Future<Void> stop();

    Future<Void> start();

    void disable();

    void enable();

}
