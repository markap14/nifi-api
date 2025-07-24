/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that can be added to a method in a Processor or ControllerService in order
 * to expose the method to connectors for invocation.
 *
 * TODO: Unclear if this really makes sense or not, but it would allow for Connectors to invoke methods
 * in Processors to list tables available, etc. such that the Connector doesn't have to re-implement all of the logic
 * in a way that may not be consistent with how the Processor works.
 */
@Documented
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ConnectorMethod {
    String name();

    String description() default "";

    MethodArgument[] arguments() default {};
}
