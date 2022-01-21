/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;


import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.executor.ThreadContext;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author dcy
 * Create Date: 2021-05-26
 */
public class FakeResponseService extends MySQLResponseService implements FakeService {
    private static final Logger LOGGER = LogManager.getLogger(FakeResponseService.class);

    public FakeResponseService(BackendConnection connection) {
        super(connection);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        LOGGER.warn("can't process this packet.{}", data);
    }

    @Override
    public void handle(ServiceTask task) {
        LOGGER.debug("can't process this packet.{}", task);
    }

    @Override
    public void execute(ServiceTask task, ThreadContext threadContext) {
        LOGGER.debug("can't process this packet.{}", task);
    }

    @Override
    public void cleanup() {
        LOGGER.debug("can't process this packet.");
    }

    @Override
    public boolean isFakeClosed() {
        return true;
    }
}
