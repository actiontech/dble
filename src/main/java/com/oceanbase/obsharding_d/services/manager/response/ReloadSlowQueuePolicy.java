/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.server.status.SlowQueryLog;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class ReloadSlowQueuePolicy {
    private ReloadSlowQueuePolicy() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSlowQueuePolicy.class);

    public static void execute(ManagerService service, int policy) {
        if (policy != 1 && policy != 2) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "slow queue overflow policy is only supported as 1 or 2");
            return;
        }
        try {
            WriteDynamicBootstrap.getInstance().changeValue("slowQueueOverflowPolicy", String.valueOf(policy));
        } catch (IOException e) {
            String msg = " reload @@slow_query.queue_policy failed";
            LOGGER.warn(service + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        SlowQueryLog.getInstance().setQueueOverflowPolicy(policy);
        LOGGER.info(service + " reload @@slow_query.queue_policy=" + policy + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("reload @@slow_query.queue_policy success".getBytes());
        ok.write(service.getConnection());
    }

}
