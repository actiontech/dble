/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.server.ServerConnection;

public final class SptDrop {
    private SptDrop() {
    }

    public static void response(ServerConnection c) {
        String name = c.getSptPrepare().getName();
        if (c.getSptPrepare().delPrepare(name)) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "Unknown prepared statement handler" + name + " given to DEALLOCATE PREPARE");
        }
    }
}
