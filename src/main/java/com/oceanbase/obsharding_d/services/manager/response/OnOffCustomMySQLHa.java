/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.singleton.CustomMySQLHa;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OnOffCustomMySQLHa {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffCustomMySQLHa.class);

    private OnOffCustomMySQLHa() {
    }

    public static void execute(ManagerService service, boolean isOn) {
        String msg;
        String onOffStatus = isOn ? "enable" : "disable";
        if (isOn) {
            msg = CustomMySQLHa.getInstance().start();
        } else {
            msg = CustomMySQLHa.getInstance().stop(false);
        }
        if (msg == null) {
            LOGGER.info(String.valueOf(service) + " " + onOffStatus + " @@custom_mysql_ha success by manager");
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage((onOffStatus + " @@custom_mysql_ha success").getBytes());
            ok.write(service.getConnection());
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }
}
