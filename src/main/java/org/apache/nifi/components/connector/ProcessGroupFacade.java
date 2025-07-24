/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
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

    void enableControllerServices();

    void disableControllerServices();

    void startProcessors();

    void stopProcessors();

    QueueSize getQueueSize();

    StatelessGroupLifecycle getStatelessLifecycle();
}
