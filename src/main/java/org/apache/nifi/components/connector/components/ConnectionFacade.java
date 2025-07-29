/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.VersionedConnection;

public interface ConnectionFacade {

    VersionedConnection getDefinition();

    /**
     * Returns the size of the Connection's queue.
     */
    QueueSize getQueueSize();

    /**
     * Purges all data from the connection.
     */
    void purge();

}
