/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.singleton.XASessionCheck;
import com.oceanbase.obsharding_d.util.SplitUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author collapsar
 */
public final class KillXASession {
    private KillXASession() {
    }

    public static void response(String stmt, int offset, ManagerService service) {
        int count = 0;
        List<NonBlockingSession> list = getList(stmt, offset);
        if (list != null) {
            count = list.size();
            for (NonBlockingSession session : list) {
                session.setRetryXa(false);
            }
        }
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(count);
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }

    private static List<NonBlockingSession> getList(String stmt, int offset) {
        String ids = stmt.substring(offset).trim();
        final XASessionCheck checker = XASessionCheck.getInstance();
        if (ids.length() > 0) {
            String[] idList = SplitUtil.split(ids, ',', true);
            List<NonBlockingSession> sessionList = new ArrayList<>(idList.length);
            for (String id : idList) {
                long value = 0;
                try {
                    value = Long.parseLong(id);
                } catch (NumberFormatException e) {
                    continue;
                }
                NonBlockingSession session = null;
                if ((session = checker.getRollbackingSession().remove(value)) != null) {
                    sessionList.add(session);
                    break;
                }
                if ((session = checker.getCommittingSession().remove(value)) != null) {
                    sessionList.add(session);
                    break;
                }
            }
            return sessionList;
        }
        return null;
    }

}
