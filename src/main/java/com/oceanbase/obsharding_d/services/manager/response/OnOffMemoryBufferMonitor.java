/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.buffer.MemoryBufferMonitor;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class OnOffMemoryBufferMonitor {
    private OnOffMemoryBufferMonitor() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffMemoryBufferMonitor.class);

    public static void execute(ManagerService service, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        try {
            WriteDynamicBootstrap.getInstance().changeValue("enableMemoryBufferMonitor", isOn ? "1" : "0");
        } catch (IOException e) {
            String msg = onOffStatus + " MemoryBufferMonitor failed";
            LOGGER.warn(String.valueOf(service) + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }

        MemoryBufferMonitor.getInstance().setEnable(isOn);
        LOGGER.info(String.valueOf(service) + " " + onOffStatus + " MemoryBufferMonitor success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " MemoryBufferMonitor success").getBytes());
        ok.write(service.getConnection());
    }

}
