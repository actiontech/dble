/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.HeartbeatPacket;
import com.actiontech.dble.net.mysql.OkPacket;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
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

    public static void response(ShardingService service, byte[] data) {
        HeartbeatPacket hp = new HeartbeatPacket();
        hp.read(data);
        if (DbleServer.getInstance().isOnline()) {
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(hp.getId());
            ok.setServerStatus(2);
            ok.write(service.getConnection());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(responseMessage("OK", service, hp.getId()));
            }
        } else {
            ErrorPacket error = new ErrorPacket();
            error.setPacketId(1);
            error.setErrNo(ErrorCode.ER_SERVER_SHUTDOWN);
            error.setMessage(String.valueOf(hp.getId()).getBytes());
            error.write(service.getConnection());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(responseMessage("ERROR", service, hp.getId()));
            }
        }
    }

    private static String responseMessage(String action, ShardingService service, long id) {
        return "RESPONSE:" + action + ", id=" + id + ", host=" +
                service.getConnection().getHost() + ", port=" + service.getConnection().getPort() + ", time=" +
                TimeUtil.currentTimeMillis();
    }

}
