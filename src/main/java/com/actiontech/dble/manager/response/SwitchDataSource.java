/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.ManagerParseSwitch;
import com.actiontech.dble.route.parser.util.Pair;

import java.util.Map;

/**
 * SwitchDataSource
 *
 * @author mycat
 */
public final class SwitchDataSource {
    private SwitchDataSource() {
    }

    public static void response(String stmt, ManagerConnection c) {
        int count = 0;
        Pair<String[], Integer> pair = ManagerParseSwitch.getPair(stmt);
        Map<String, PhysicalDBPool> dns = DbleServer.getInstance().getConfig().getDataHosts();
        Integer idx = pair.getValue();
        for (String key : pair.getKey()) {
            PhysicalDBPool dn = dns.get(key);
            if (dn != null) {
                int m = dn.getActiveIndex();
                int n = (idx == null) ? dn.next(m) : idx;
                if (dn.switchSource(n, false, "MANAGER")) {
                    ++count;
                }
                //TODO:ELSE?
            }
        }
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(count);
        packet.setServerStatus(2);
        packet.write(c);
    }

}
