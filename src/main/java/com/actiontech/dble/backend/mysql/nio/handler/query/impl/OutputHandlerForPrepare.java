/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.nio.handler.util.HandlerTool;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;

import java.util.List;

/*
 * send back to client handler
 */
public class OutputHandlerForPrepare extends OutputHandler {


    public OutputHandlerForPrepare(long id, Session session) {
        super(id, session);

    }




    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets, byte[] eofNull, boolean isLeft, AbstractService service) {
        this.fieldPackets = fieldPackets;
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, AbstractService service) {
        requestScope.getCurrentPreparedStatement().onPrepareOk(fieldPackets.size());
        HandlerTool.terminateHandlerTree(this);
        serverSession.setHandlerEnd(this);
        serverSession.setResponseTime(true);
        return;

    }


}
