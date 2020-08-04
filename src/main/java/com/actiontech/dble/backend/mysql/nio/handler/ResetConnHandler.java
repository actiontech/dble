/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ResetConnHandler implements ResponseHandler {
    public static final Logger LOGGER = LoggerFactory.getLogger(ResetConnHandler.class);

    @Override
    public void connectionError(Throwable e, Object attachment) {
        String msg = e.getMessage() == null ? e.toString() : e.getMessage();
        LOGGER.info(msg);
    }


    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        service.getConnection().close(new String(errPg.getMessage()));
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        ((MySQLResponseService) service).resetContextStatus();
        ((MySQLResponseService) service).release();
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        //not happen
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        LOGGER.info(reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        //not happen
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        //not happen
    }

}
