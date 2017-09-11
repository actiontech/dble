/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.HeartbeatPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public final class Heartbeat {
    private Heartbeat() {
    }

    private static final Logger HEARTBEAT = LoggerFactory.getLogger("heartbeat");

    public static void response(ServerConnection c, byte[] data) {
        HeartbeatPacket hp = new HeartbeatPacket();
        hp.read(data);
        if (DbleServer.getInstance().isOnline()) {
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(hp.getId());
            ok.setServerStatus(2);
            ok.write(c);
            if (HEARTBEAT.isInfoEnabled()) {
                HEARTBEAT.info(responseMessage("OK", c, hp.getId()));
            }
        } else {
            ErrorPacket error = new ErrorPacket();
            error.setPacketId(1);
            error.setErrno(ErrorCode.ER_SERVER_SHUTDOWN);
            error.setMessage(String.valueOf(hp.getId()).getBytes());
            error.write(c);
            if (HEARTBEAT.isInfoEnabled()) {
                HEARTBEAT.info(responseMessage("ERROR", c, hp.getId()));
            }
        }
    }

    private static String responseMessage(String action, ServerConnection c, long id) {
        return "RESPONSE:" + action + ", id=" + id + ", host=" +
                c.getHost() + ", port=" + c.getPort() + ", time=" +
                TimeUtil.currentTimeMillis();
    }

}
