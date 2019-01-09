/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.CheckFullMetaData;
import com.actiontech.dble.manager.response.ShowTime;
import com.actiontech.dble.route.parser.ManagerParseCheck;

/**
 * @author huqing.yan
 */
public final class CheckHandler {
    private CheckHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseCheck.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseCheck.META_DATA:
                ShowTime.execute(c, DbleServer.getInstance().getTmManager().getTimestamp());
                break;
            case ManagerParseCheck.FULL_META_DATA:
                CheckFullMetaData.execute(c, stmt);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
