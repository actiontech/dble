/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

public final class SptDrop {
    private SptDrop() {
    }

    public static void response(ShardingService service) {
        String name = service.getSptPrepare().getName();
        if (service.getSptPrepare().delPrepare(name)) {
            service.writeOkPacket();
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "Unknown prepared statement handler" + name + " given to DEALLOCATE PREPARE");
        }
    }
}
