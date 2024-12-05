/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.PacketUtil;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;

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
        if (OBsharding_DServer.getInstance().isOnline()) {
            c.getService().write(OkPacket.getDefault());
        } else {
            ERROR.write(c);
        }
    }

}
