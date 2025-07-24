/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.util.concurrent.Future;

public interface ProcessorLifecycle {

    ProcessorState getState();

    int getActiveThreadCount();

    void terminate();

    Future<Void> stop();

    Future<Void> start();

    void disable();

    void enable();

    @ConnectorMethod(
        name = "test",
        description = "Runs a test on the processor to validate its configuration and functionality.",
        arguments = {
            @MethodArgument(name = "name", type = String.class, description = "The name of the test to run."),
            @MethodArgument(name = "iterations", type = int.class, description = "The number of iterations to run the test.")
        }
    )
    void test(String name, int iterations);
}
