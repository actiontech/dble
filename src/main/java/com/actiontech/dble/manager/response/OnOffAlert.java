/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.handler.WriteDynamicBootstrap;
import com.actiontech.dble.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author bfq
 */
public final class OnOffAlert {
    private OnOffAlert() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OnOffAlert.class);

    public static void execute(ManagerConnection c, boolean isOn) {
        String onOffStatus = isOn ? "enable" : "disable";
        try {
            WriteDynamicBootstrap.getInstance().changeValue("enableAlert", isOn ? "1" : "0");
        } catch (IOException e) {
            String msg = onOffStatus + " alert failed";
            LOGGER.warn(String.valueOf(c) + " " + msg, e);
            c.writeErrMessage(ErrorCode.ER_YES, msg);
            return;
        }
        AlertUtil.switchAlert(isOn);
        LOGGER.info(String.valueOf(c) + " " + onOffStatus + " alert success by manager");

        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage((onOffStatus + " alert success").getBytes());
        ok.write(c);
    }

}
