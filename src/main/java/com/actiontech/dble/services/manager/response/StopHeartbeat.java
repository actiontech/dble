/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.ManagerParseStop;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.FormatUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * StopHeartbeatCheck
 *
 * @author mycat
 */
public final class StopHeartbeat {
    private StopHeartbeat() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(StopHeartbeat.class);

    public static void execute(String stmt, ManagerService service) {
        int count = 0;
        Pair<String[], Integer> keys = ManagerParseStop.getPair(stmt);
        if (keys.getKey() != null && keys.getValue() != null) {
            long time = keys.getValue() * 1000L;
            Map<String, PhysicalDbGroup> dns = DbleServer.getInstance().getConfig().getDbGroups();
            for (String key : keys.getKey()) {
                PhysicalDbGroup dn = dns.get(key);
                if (dn != null) {
                    dn.getWriteDbInstance().setHeartbeatRecoveryTime(TimeUtil.currentTimeMillis() + time);
                    ++count;
                    StringBuilder s = new StringBuilder();
                    s.append(dn.getGroupName()).append(" stop heartbeat '");
                    LOGGER.info(s.append(FormatUtil.formatTime(time, 3)).append("' by manager.").toString());
                }
            }
        }
        OkPacket packet = new OkPacket();
        packet.setPacketId(service.nextPacketId());
        packet.setAffectedRows(count);
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }

}
