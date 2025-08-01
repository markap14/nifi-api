/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.util.concurrent.Future;

public interface ProcessGroupLifecycle {

    Future<Void> enableControllerServices();

    Future<Void> disableControllerServices();

    void startProcessors();

    Future<Void> stopProcessors();

}
