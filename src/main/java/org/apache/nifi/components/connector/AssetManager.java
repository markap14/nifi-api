/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.io.IOException;
import java.io.InputStream;

public interface AssetManager {

    /**
     * Stores the asset with the given name and content, returning the path where the asset is stored.
     *
     * @param assetName the name of the asset to store
     * @param content the contents of the asset as an InputStream
     * @return the path where the asset is stored
     * @throws IOException if unable to read the provided content or write to the underlying storage mechanism
     */
    String storeAsset(String assetName, InputStream content) throws IOException;

}
