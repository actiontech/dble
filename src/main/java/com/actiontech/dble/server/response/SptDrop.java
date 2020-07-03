/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.mysqlsharding.ShardingService;

public final class SptDrop {
    private SptDrop() {
    }

    public static void response(ShardingService service) {
        String name = service.getSptPrepare().getName();
        if (service.getSptPrepare().delPrepare(name)) {
            service.writeDirectly(service.writeToBuffer(OkPacket.OK, service.allocate()));
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "Unknown prepared statement handler" + name + " given to DEALLOCATE PREPARE");
        }
    }
}
