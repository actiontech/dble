/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;

public class ManagerCommandHandler extends FrontendCommandHandler {
    ManagerCommandHandler(ManagerConnection source) {
        super(source);
    }
    protected void handleDataByPacket(byte[] data) {
        dataTodo = data;
        DbleServer.getInstance().getFrontHandlerQueue().offer(this);
    }
    @Override
    protected void handleData(byte[] data) {
        ManagerConnection mc = (ManagerConnection) source;
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                mc.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                mc.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                mc.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                mc.close("quit cmd");
                break;
            default:
                commands.doOther();
                mc.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
