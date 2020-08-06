/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.status.SlowQueryLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class ReloadSlowQueryFlushSize {
    private ReloadSlowQueryFlushSize() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSlowQueryFlushSize.class);

    public static void execute(ManagerService service, int size) {
        if (size < 0) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "the commend is not correct");
            return;
        }
        try {
            WriteDynamicBootstrap.getInstance().changeValue("flushSlowLogSize", String.valueOf(size));
        } catch (IOException e) {
            String msg = " reload @@slow_query.flushSize failed";
            LOGGER.warn(String.valueOf(service) + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        SlowQueryLog.getInstance().setFlushSize(size);
        LOGGER.info(String.valueOf(service) + " reload @@slow_query.flushSize=" + size + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("reload @@slow_query.flushSize success".getBytes());
        ok.write(service.getConnection());
    }

}
