package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.singleton.AppendTraceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class OnOffAppendTraceId {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffAppendTraceId.class);

    private OnOffAppendTraceId() {
    }

    public static void execute(ManagerService service, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        try {
            WriteDynamicBootstrap.getInstance().changeValue("appendTraceId", isOn ? "1" : "0");
        } catch (IOException e) {
            String msg = onOffStatus + " appendTraceId failed";
            LOGGER.warn(String.valueOf(service) + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }

        AppendTraceId.getInstance().setValue(isOn ? 1 : 0);
        LOGGER.info(String.valueOf(service) + " " + onOffStatus + " appendTraceId success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " appendTraceId success").getBytes());
        ok.write(service.getConnection());

    }

}
