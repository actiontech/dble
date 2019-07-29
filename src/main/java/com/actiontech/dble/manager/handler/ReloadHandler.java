/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.*;
import com.actiontech.dble.route.parser.ManagerParseReload;
import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ReloadHandler {
    private static final int SHIFT = 8;

    private ReloadHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseReload.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseReload.CONFIG:
                ReloadConfig.execute(c, stmt, rs >>> SHIFT);
                break;
            case ManagerParseReload.USER_STAT:
                ReloadUserStat.execute(c);
                break;
            case ManagerParseReload.SQL_SLOW:
                ReloadSqlSlowTime.execute(c, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_TIME:
                ReloadSlowQueryTime.execute(c, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_FLUSH_PERIOD:
                ReloadSlowQueryFlushPeriod.execute(c, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_FLUSH_SIZE:
                ReloadSlowQueryFlushSize.execute(c, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.QUERY_CF:
                String filter = ParseUtil.parseString(stmt);
                ReloadQueryCf.execute(c, filter);
                break;
            case ManagerParseReload.META_DATA:
                String whereCondition = stmt.substring(rs >>> SHIFT).trim();
                ReloadMetaData.execute(c, whereCondition);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
