/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;

/**
 * @author mycat
 */
public final class KillHandler {
    private KillHandler() {
    }

    public static void handle(String stmt, int offset, ServerConnection c) {
        String id = stmt.substring(offset).trim();
        if (StringUtil.isEmpty(id)) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
        } else {
            // get value
            long value = 0;
            try {
                value = Long.parseLong(id);
            } catch (NumberFormatException e) {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
                return;
            }

            // kill myself
            if (value == c.getId()) {
                getOkPacket(c).write(c);
                c.write(c.allocate());
                return;
            }

            // get connection and close it
            FrontendConnection fc = null;
            NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
            for (NIOProcessor p : processors) {
                if ((fc = p.getFrontends().get(value)) != null) {
                    break;
                }
            }
            if (fc != null) {
                if (!fc.getUser().equals(c.getUser())) {
                    c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection" + id);
                    return;
                }
                fc.killAndClose("killed");
                boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
                getOkPacket(c).write(c);
                c.getSession2().multiStatementNextSql(multiStatementFlag);
            } else {
                c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            }
        }
    }

    private static OkPacket getOkPacket(ServerConnection c) {
        byte packetId = (byte) c.getSession2().getPacketId().get();
        OkPacket packet = new OkPacket();
        packet.setPacketId(packetId);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        c.getSession2().multiStatementPacket(packet, packetId);
        return packet;
    }

}
