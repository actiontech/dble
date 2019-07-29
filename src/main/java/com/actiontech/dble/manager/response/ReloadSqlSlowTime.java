/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class ReloadSqlSlowTime {
    private ReloadSqlSlowTime() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c, int time) {
        if (time < 0) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the commend is not correct");
            return;
        }

        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            userStat.setSlowTime(time);
        }

        LOGGER.info(String.valueOf(c) + " reload @@sqlslow=" + time + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("reload @@sqlslow success".getBytes());
        ok.write(c);
    }

}
