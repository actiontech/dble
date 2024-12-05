/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.handler.WriteDynamicBootstrap;
import com.oceanbase.obsharding_d.singleton.CapClientFoundRows;
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
