/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.ManagerParseReload;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.*;

/**
 * @author mycat
 */
public final class ReloadHandler {
    private static final int SHIFT = 8;

    private ReloadHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseReload.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseReload.CONFIG:
                ReloadConfig.execute(service, stmt, rs >>> SHIFT);
                break;
            case ManagerParseReload.SAMPLING_RATE:
                StatisticCf.SamplingSwitch.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_TIME:
                ReloadSlowQueryTime.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_FLUSH_PERIOD:
                ReloadSlowQueryFlushPeriod.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_FLUSH_SIZE:
                ReloadSlowQueryFlushSize.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.SLOW_QUERY_QUEUE_POLICY:
                ReloadSlowQueuePolicy.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.QUERY_CF:
                String filter = ParseUtil.parseString(stmt);
                ReloadQueryCf.execute(service, filter);
                break;
            case ManagerParseReload.META_DATA:
                String whereCondition = stmt.substring(rs >>> SHIFT).trim();
                ReloadMetaData.execute(service, whereCondition);
                break;
            case ManagerParseReload.GENERAL_LOG_FILE:
                GeneralLogCf.ReloadGeneralLogFile.execute(service, ParseUtil.parseString(stmt));
                break;
            case ManagerParseReload.STATISTIC_TABLE_SIZE:
                StatisticCf.SetTableMaxSize.execute(service, ParseUtil.parseString(stmt));
                break;
            case ManagerParseReload.LOAD_DATA_NUM:
                ReloadLoadDataNum.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            case ManagerParseReload.XAID_CHECK_PERIOD:
                XaidCheck.execute(service, ParseUtil.getSQLId(stmt, rs >>> SHIFT));
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }

}
