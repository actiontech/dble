/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.SplitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class KillConnection {
    private KillConnection() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KillConnection.class);

    public static void response(String stmt, int offset, ManagerConnection mc) {
        int count = 0;
        List<FrontendConnection> list = getList(stmt, offset);
        if (list != null) {
            for (FrontendConnection c : list) {
                StringBuilder s = new StringBuilder();
                LOGGER.info(s.append(c).append("killed by manager").toString());
                c.killAndClose("kill by manager");
                count++;
            }
        }
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(count);
        packet.setServerStatus(2);
        packet.write(mc);
    }

    private static List<FrontendConnection> getList(String stmt, int offset) {
        String ids = stmt.substring(offset).trim();
        if (ids.length() > 0) {
            String[] idList = SplitUtil.split(ids, ',', true);
            List<FrontendConnection> fcList = new ArrayList<>(idList.length);
            NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
            for (String id : idList) {
                long value = 0;
                try {
                    value = Long.parseLong(id);
                } catch (NumberFormatException e) {
                    continue;
                }
                FrontendConnection fc = null;
                for (NIOProcessor p : processors) {
                    if ((fc = p.getFrontends().get(value)) != null) {
                        fcList.add(fc);
                        break;
                    }
                }
            }
            return fcList;
        }
        return null;
    }

}
