/*
* Copyright (C) 2016-2021 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.WriteFlags;

/**
 * for heartbeat.
 *
 * @author mycat
 */
public final class Ping {
    private Ping() {
    }

    private static final ErrorPacket ERROR = PacketUtil.getShutdown();


    public static void response(AbstractConnection c) {
        if (DbleServer.getInstance().isOnline()) {
            c.getService().writeDirectly(c.getService().writeToBuffer(OkPacket.getDefault(), c.allocate()), WriteFlags.QUERY_END);
        } else {
            ERROR.write(c);
        }
    }

}
