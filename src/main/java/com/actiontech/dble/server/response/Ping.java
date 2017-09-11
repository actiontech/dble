/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;

/**
 * for heartbeat.
 *
 * @author mycat
 */
public final class Ping {
    private Ping() {
    }

    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    public static void response(FrontendConnection c) {
        if (DbleServer.getInstance().isOnline()) {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        } else {
            ERROR.write(c);
        }
    }

}
