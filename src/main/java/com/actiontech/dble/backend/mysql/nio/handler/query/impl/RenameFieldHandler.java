/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.List;

public class RenameFieldHandler extends BaseDMLHandler {
    private String alias;
    public RenameFieldHandler(long id, NonBlockingSession session, String alias) {
        super(id, session);
        this.alias = alias;
    }

    @Override
    public HandlerType type() {
        return HandlerType.RENAME_FIELD;
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        for (FieldPacket fp : fieldPackets) {
            fp.setTable(alias.getBytes());
            //TODO:ORGTABLENAME
        }
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        nextHandler.rowResponse(rowNull, rowPacket, this.isLeft, conn);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        nextHandler.rowEofResponse(eof, this.isLeft, conn);
    }

    @Override
    protected void onTerminate() throws Exception {
    }
}
