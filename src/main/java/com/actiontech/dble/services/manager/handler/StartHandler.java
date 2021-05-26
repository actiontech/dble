/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.ManagerParseStart;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.StatisticCf;

/**
 * @author mycat
 */
public final class StartHandler {
    private StartHandler() {
    }

    public static void handle(String stmt, ManagerService c, int offset) {
        switch (ManagerParseStart.parse(stmt, offset)) {
            case ManagerParseStart.STATISTIC_QUEUE_MONITOR:
                StatisticCf.Queue.start(c, stmt);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
