/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
