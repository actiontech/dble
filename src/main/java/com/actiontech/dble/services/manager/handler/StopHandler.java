/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.StopHeartbeat;
import com.actiontech.dble.route.parser.ManagerParseStop;

/**
 * @author mycat
 */
public final class StopHandler {
    private StopHandler() {
    }

    public static void handle(String stmt, ManagerService c, int offset) {
        switch (ManagerParseStop.parse(stmt, offset)) {
            case ManagerParseStop.HEARTBEAT:
                StopHeartbeat.execute(stmt, c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
