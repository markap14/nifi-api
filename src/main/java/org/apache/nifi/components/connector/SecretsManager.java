/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.io.IOException;
import java.util.List;

public interface SecretsManager {

    List<Secret> getSecrets() throws IOException;

}
