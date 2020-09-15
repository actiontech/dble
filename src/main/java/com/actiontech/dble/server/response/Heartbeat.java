/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.HeartbeatPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mycat
 */
public final class Heartbeat {
    private Heartbeat() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Heartbeat.class);

    public static void response(AbstractConnection conn, byte[] data) {
        HeartbeatPacket hp = new HeartbeatPacket();
        hp.read(data);
        if (DbleServer.getInstance().isOnline()) {
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(hp.getId());
            ok.setServerStatus(2);
            ok.write(conn);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(responseMessage("OK", conn, hp.getId()));
            }
        } else {
            ErrorPacket error = new ErrorPacket();
            error.setPacketId(1);
            error.setErrNo(ErrorCode.ER_SERVER_SHUTDOWN);
            error.setMessage(String.valueOf(hp.getId()).getBytes());
            error.write(conn);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(responseMessage("ERROR", conn, hp.getId()));
            }
        }
    }

    private static String responseMessage(String action, AbstractConnection conn, long id) {
        return "RESPONSE:" + action + ", id=" + id + ", host=" +
                conn.getHost() + ", port=" + conn.getPort() + ", time=" +
                TimeUtil.currentTimeMillis();
    }

}
