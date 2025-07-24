/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

/**
 * Annotation that can be provided as part of a {@link ConnectorMethod} definition to describe the arguments
 * that the method accepts. This annotation is used to provide metadata about the method's arguments
 * to facilitate dynamic invocation and documentation generation.
 */
public @interface MethodArgument {
    String name();

    Class<?> type();

    String description() default "";

    boolean required() default true;
}
