/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.singleton.XASessionCheck;
import com.actiontech.dble.util.SplitUtil;

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
