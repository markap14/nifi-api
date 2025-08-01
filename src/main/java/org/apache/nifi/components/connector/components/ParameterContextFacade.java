/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import java.io.IOException;
import java.io.InputStream;

public interface ParameterContextFacade {

    /**
     * Sets the value of a parameter in the Parameter Context.
     * @param parameterName the name of the parameter to set
     * @param value the value to set for the parameter
     * @return the previous value of the parameter, or null if it was not set before
     */
    String setValue(String parameterName, String value);

    /**
     * Gets the value of a parameter from the Parameter Context.
     * @param parameterName the name of the parameter to retrieve
     * @return the value of the parameter, or null if it is not set
     */
    String getValue(String parameterName);

    /**
     * Creates an asset whose contents are provided by the given InputStream.
     * @param parameterName the name of the parameter to assign the asset to
     * @param inputStream the InputStream containing the asset contents
     * @throws IOException if an error occurs while reading from the InputStream or storing the asset
     */
    void createAsset(String parameterName, InputStream inputStream) throws IOException;
}
