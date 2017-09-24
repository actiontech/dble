/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReloadMetaData {
    private ReloadMetaData() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadMetaData.class);

    public static void execute(ManagerConnection c) {
        String msg = "datahost has no write_host";
        if (!DbleServer.getInstance().getConfig().isDataHostWithoutWR()) {
            DbleServer.getInstance().reloadMetaData(DbleServer.getInstance().getConfig());
            msg = "reload metadata sucess";
        }
        LOGGER.info(msg);
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(1);
        ok.setServerStatus(2);
        ok.setMessage(msg.getBytes());
        ok.write(c);
    }
}
