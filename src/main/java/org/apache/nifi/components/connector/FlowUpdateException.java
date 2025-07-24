/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

public class FlowUpdateException extends Exception {
    public FlowUpdateException(final String message) {
        super(message);
    }

    public FlowUpdateException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public FlowUpdateException(final Throwable cause) {
        super(cause);
    }
}
