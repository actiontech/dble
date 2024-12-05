/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.route.parser.ManagerParseStop;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.FormatUtil;
import com.oceanbase.obsharding_d.util.TimeUtil;
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
            Map<String, PhysicalDbGroup> dns = OBsharding_DServer.getInstance().getConfig().getDbGroups();
            for (String key : keys.getKey()) {
                PhysicalDbGroup dn = dns.get(key);
                if (dn != null) {
                    dn.getWriteDbInstance().getHeartbeat().setHeartbeatRecoveryTime(TimeUtil.currentTimeMillis() + time);
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
