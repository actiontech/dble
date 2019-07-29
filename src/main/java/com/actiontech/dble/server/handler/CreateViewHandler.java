/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;

/**
 * Created by szf on 2017/10/9.
 */
public final class CreateViewHandler {

    private CreateViewHandler() {
    }

    public static void handle(String stmt, ServerConnection c, boolean isReplace) {
        //create a new object of the view
        ViewMeta vm = new ViewMeta(stmt, c.getSchema(), DbleServer.getInstance().getTmManager());
        ErrorPacket error = vm.initAndSet(isReplace, true);
        if (error != null) {
            //if any error occurs when parse sql into view object
            c.writeErrMessage(error.getErrNo(), new String(error.getMessage()));
            return;
        }


        //if the create success with no error send back OK
        byte packetId = (byte) c.getSession2().getPacketId().get();
        OkPacket ok = new OkPacket();
        ok.setPacketId(++packetId);
        c.getSession2().multiStatementPacket(ok, packetId);
        ok.write(c);
        boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
        c.getSession2().multiStatementNextSql(multiStatementFlag);
        return;
    }
}
