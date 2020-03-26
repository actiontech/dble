/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.singleton.CustomMySQLHa;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OnOffCustomMySQLHa {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffCustomMySQLHa.class);

    private OnOffCustomMySQLHa() {
    }

    public static void execute(ManagerConnection c, boolean isOn) {
        String msg;
        String onOffStatus = isOn ? "enable" : "disable";
        if (isOn) {
            msg = CustomMySQLHa.getInstance().start();
        } else {
            msg = CustomMySQLHa.getInstance().stop(false);
        }
        if (msg == null) {
            LOGGER.info(String.valueOf(c) + " " + onOffStatus + " @@custom_mysql_ha success by manager");
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage((onOffStatus + " @@custom_mysql_ha success").getBytes());
            ok.write(c);
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }
}
