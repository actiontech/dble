/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.util.SplitUtil;
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

    public static void response(String stmt, int offset, ManagerService service) {
        int count = 0;
        List<FrontendConnection> list = getList(stmt, offset);
        if (list != null) {
            for (FrontendConnection c : list) {
                StringBuilder s = new StringBuilder();
                LOGGER.info(s.append(c).append("killed by manager").toString());
                c.getFrontEndService().killAndClose("kill by manager");
                count++;
            }
        }
        OkPacket packet = new OkPacket();
        packet.setPacketId(service.nextPacketId());
        packet.setAffectedRows(count);
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }

    private static List<FrontendConnection> getList(String stmt, int offset) {
        String ids = stmt.substring(offset).trim();
        if (ids.length() > 0) {
            String[] idList = SplitUtil.split(ids, ',', true);
            List<FrontendConnection> fcList = new ArrayList<>(idList.length);
            IOProcessor[] processors = OBsharding_DServer.getInstance().getFrontProcessors();
            for (String id : idList) {
                long value = 0;
                try {
                    value = Long.parseLong(id);
                } catch (NumberFormatException e) {
                    continue;
                }
                FrontendConnection fc = null;
                for (IOProcessor p : processors) {
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
