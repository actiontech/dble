/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.util.HandlerTool;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/*
 * send back to client handler
 */
public class OutputHandlerForPrepare extends OutputHandler {


    public OutputHandlerForPrepare(long id, Session session) {
        super(id, session);

    }


    @Override
    public void fieldEofResponse(byte[] headerNull, List<byte[]> fieldsNull, List<FieldPacket> fieldPackets, byte[] eofNull, boolean isLeft, @NotNull AbstractService service) {
        this.fieldPackets = fieldPackets;
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        requestScope.getCurrentPreparedStatement().onPrepareOk(fieldPackets.size());
        HandlerTool.terminateHandlerTree(this);
        serverSession.setHandlerEnd(this);
        return;

    }


}
