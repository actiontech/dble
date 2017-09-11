/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.StopHeartbeat;
import com.actiontech.dble.route.parser.ManagerParseStop;

/**
 * @author mycat
 */
public final class StopHandler {
    private StopHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        switch (ManagerParseStop.parse(stmt, offset)) {
            case ManagerParseStop.HEARTBEAT:
                StopHeartbeat.execute(stmt, c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
