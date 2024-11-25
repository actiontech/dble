/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.route.parser.ManagerParseStart;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.StatisticCf;

/**
 * @author mycat
 */
public final class StartHandler {
    private StartHandler() {
    }

    public static void handle(String stmt, ManagerService c, int offset) {
        int rs = ManagerParseStart.parse(stmt, offset);
        int sqlType = rs & 0xff;
        switch (sqlType) {
            case ManagerParseStart.STATISTIC_QUEUE_MONITOR:
                StatisticCf.Queue.start(c, stmt, rs >>> 8);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
