/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.status.SlowQueryLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class ReloadSlowQueryFlushPeriod {
    private ReloadSlowQueryFlushPeriod() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSlowQueryFlushPeriod.class);

    public static void execute(ManagerConnection c, int time) {
        if (time < 0) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the commend is not correct");
            return;
        }
        try {
            WriteDynamicBootstrap.getInstance().changeValue("flushSlowLogPeriod", String.valueOf(time));
        } catch (IOException e) {
            String msg = "reload @@slow_query.flushPeriod failed";
            LOGGER.warn(String.valueOf(c) + " " + msg, e);
            c.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        SlowQueryLog.getInstance().setFlushPeriod(time);
        LOGGER.info(String.valueOf(c) + " reload @@slow_query.flushPeriod=" + time + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("reload @@slow_query.flushPeriod success".getBytes());
        ok.write(c);
    }

}
