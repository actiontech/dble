/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;

import java.util.List;

/**
 * @author mycat
 */
public class DelegateResponseHandler implements ResponseHandler {
    private final ResponseHandler target;

    public DelegateResponseHandler(ResponseHandler target) {
        if (target == null) {
            throw new IllegalArgumentException("delegate is null!");
        }
        this.target = target;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        target.connectionAcquired(conn);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        target.connectionError(e, conn);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        target.okResponse(ok, conn);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        target.errorResponse(err, conn);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        target.fieldEofResponse(header, fields, fieldPackets, eof, isLeft, conn);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        return target.rowResponse(row, rowPacket, isLeft, conn);
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        target.rowEofResponse(eof, isLeft, conn);
    }

    @Override
    public void writeQueueAvailable() {
        target.writeQueueAvailable();

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        target.connectionClose(conn, reason);
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
    }
}
