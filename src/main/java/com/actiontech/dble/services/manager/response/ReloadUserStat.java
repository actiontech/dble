/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class ReloadUserStat {
    private ReloadUserStat() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadUserStat.class);

    public static void execute(ManagerService service) {

        Map<UserName, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            userStat.reset();
        }

        LOGGER.info(String.valueOf(service) + "Reset show @@sql  @@sql.sum  @@sql.slow  @@sql.high  @@sql.large  @@sql.resultset success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("Reset show @@sql  @@sql.sum @@sql.slow  @@sql.high  @@sql.large  @@sql.resultset  success".getBytes());
        ok.write(service.getConnection());
    }

}
