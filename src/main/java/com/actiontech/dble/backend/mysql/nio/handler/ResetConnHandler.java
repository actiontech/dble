/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ResetConnHandler implements ResponseHandler {
    public static final Logger LOGGER = LoggerFactory.getLogger(ResetConnHandler.class);
    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        String msg = e.getMessage() == null ? e.toString() : e.getMessage();
        LOGGER.warn(msg);
    }


    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        conn.close(new String(errPg.getMessage()));
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        MySQLConnection mysqlConn = (MySQLConnection) conn;
        mysqlConn.resetContextStatus();
        conn.release();
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        //not happen
    }
    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn(reason);
    }
    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        //not happen
    }

    @Override
    public boolean rowResponse(byte[] rownull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        //not happen
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
        //not happen
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
        //not happen
    }

    @Override
    public void writeQueueAvailable() {
        //not happen
    }

}
