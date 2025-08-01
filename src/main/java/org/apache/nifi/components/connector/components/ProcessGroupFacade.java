/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.components;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Set;

public interface ProcessGroupFacade {

    VersionedProcessGroup getDefinition();

    ProcessorFacade getProcessor(String id);

    Set<ProcessorFacade> getProcessors();

    ControllerServiceFacade getControllerService(String id);

    Set<ControllerServiceFacade> getControllerServices();

    ConnectionFacade getConnection(String id);

    Set<ConnectionFacade> getConnections();

    ProcessGroupFacade getProcessGroup(String id);

    Set<ProcessGroupFacade> getProcessGroups();

    QueueSize getQueueSize();

    StatelessGroupLifecycle getStatelessLifecycle();

    ProcessGroupLifecycle getLifecycle();

}
