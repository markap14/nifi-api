/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

public class InvocationFailedException extends Exception {

    public InvocationFailedException(final String message) {
        super(message);
    }

    public InvocationFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvocationFailedException(final Throwable cause) {
        super(cause);
    }

}
