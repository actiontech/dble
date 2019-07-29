/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.status.SlowQueryLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OnOffSlowQueryLog {
    private OnOffSlowQueryLog() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffSlowQueryLog.class);

    public static void execute(ManagerConnection c, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        SlowQueryLog.getInstance().setEnableSlowLog(isOn);
        LOGGER.info(String.valueOf(c) + " " + onOffStatus + " slow_query_log success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " slow_query_log success").getBytes());
        ok.write(c);
    }

}
