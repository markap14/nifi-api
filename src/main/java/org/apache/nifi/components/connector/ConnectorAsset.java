/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Supplier;

public interface ConnectorAsset {

    String getName();

    Optional<String> getDescription();

    Optional<String> getContentType();

    Supplier<InputStream> getContentSupplier();

}
