/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;

/**
 * ManagerCommandHandler
 *
 * @author mycat
 */
public class ManagerCommandHandler extends FrontendCommandHandler {
    public ManagerCommandHandler(FrontendConnection source) {
        super(source);
    }

    @Override
    protected void handleData(byte[] data) {
        switch (data[4]) {
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                source.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                source.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                source.close("quit cmd");
                break;
            default:
                commands.doOther();
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
