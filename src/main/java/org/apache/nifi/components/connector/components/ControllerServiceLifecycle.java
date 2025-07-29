/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.util.concurrent.Future;

public interface ControllerServiceLifecycle {

    ControllerServiceState getState();

    Future<Void> enable();

    Future<Void> disable();

}
