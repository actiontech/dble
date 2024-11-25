/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.backend.mysql.xa.XaCheckHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class XaidCheck {
    private static final Logger LOGGER = LoggerFactory.getLogger(XaidCheck.class);

    private XaidCheck() {
    }

    public static void execute(ManagerService service, int value) {
        if (value <= 0) value = -1;
        try {
            WriteDynamicBootstrap.getInstance().changeValue("xaIdCheckPeriod", String.valueOf(value));
        } catch (IOException e) {
            String msg = " reload @@xaIdCheck.period failed";
            LOGGER.warn(service + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        XaCheckHandler.adjustXaIdCheckPeriod(value);
        LOGGER.info(service + " reload @@xaIdCheck.period=" + value + " success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage("reload @@xaIdCheck.period success".getBytes());
        ok.write(service.getConnection());
    }
}
