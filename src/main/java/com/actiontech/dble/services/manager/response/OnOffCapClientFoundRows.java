package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.singleton.CapClientFoundRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class OnOffCapClientFoundRows {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffCapClientFoundRows.class);

    private OnOffCapClientFoundRows() {
    }

    public static void execute(ManagerService service, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        try {
            WriteDynamicBootstrap.getInstance().changeValue("capClientFoundRows", isOn ? "true" : "false");
        } catch (IOException e) {
            String msg = onOffStatus + " cap_client_found_rows failed";
            LOGGER.warn(String.valueOf(service) + " " + msg, e);
            service.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }

        CapClientFoundRows.getInstance().setEnableCapClientFoundRows(isOn);
        LOGGER.info(String.valueOf(service) + " " + onOffStatus + " @@cap_client_found_rows success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " @@cap_client_found_rows success").getBytes());
        ok.write(service.getConnection());

    }

}
