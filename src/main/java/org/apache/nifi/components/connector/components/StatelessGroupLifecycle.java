/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.util.concurrent.Future;

public interface StatelessGroupLifecycle {

    Future<Void> start();

    Future<Void> stop();

    Future<Void> terminate();

}
