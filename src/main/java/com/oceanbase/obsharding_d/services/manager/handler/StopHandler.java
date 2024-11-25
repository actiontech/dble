/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.StatisticCf;
import com.oceanbase.obsharding_d.services.manager.response.StopHeartbeat;
import com.oceanbase.obsharding_d.route.parser.ManagerParseStop;

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
            case ManagerParseStop.STATISTIC_QUEUE_MONITOR:
                StatisticCf.Queue.stop(c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
