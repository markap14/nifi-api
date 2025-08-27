/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;

public interface ProcessorFacade {

    VersionedProcessor getDefinition();

    ProcessorLifecycle getLifecycle();

    List<ValidationResult> validate(Map<String, String> propertyValues);

    Object invokeConnectorMethod(String methodName, Map<String, Object> arguments) throws InvocationFailedException;

}
