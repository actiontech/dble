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
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.server.ServerConnection;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;

import java.sql.SQLSyntaxErrorException;

/**
 * @author mycat
 */
public final class KillHandler {
    private KillHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        long id;
        MySqlKillStatement.Type type;
        try {
            MySqlKillStatement statement = (MySqlKillStatement) RouteStrategyFactory.getRouteStrategy().parserSQL(stmt);
            id = Long.parseLong(statement.getThreadId().toString());
            type = statement.getType();
        } catch (NumberFormatException nfe) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "invalid connection id");
            return;
        } catch (SQLSyntaxErrorException se) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, se.getMessage());
            return;
        }

        // get connection and close it
        FrontendConnection fc = null;
        // is myself
        if (id == c.getId()) {
            fc = c;
        } else {
            NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
            for (NIOProcessor p : processors) {
                if ((fc = p.getFrontends().get(id)) != null)
                    break;
            }
        }

        if (fc == null) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id[" + id + "]");
            return;
        } else if (!fc.getUser().equals(c.getUser())) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection[id=" + id + "]");
            return;
        } else {
            // kill myself
            if (c == fc) {
                getOkPacket(c).write(c);
                c.write(c.allocate());
                return;
            }

            if (type == null || type == MySqlKillStatement.Type.CONNECTION) {
                // kill connection
                fc.killAndClose("killed");
            } else {
                ((ServerConnection) fc).getSession2().kill(true);
            }
        }

        // return
        getOkPacket(c).write(c);
        c.getSession2().multiStatementNextSql(c.getSession2().getIsMultiStatement().get());
    }

    private static OkPacket getOkPacket(ServerConnection c) {
        byte packetId = (byte) c.getSession2().getPacketId().get();
        OkPacket packet = new OkPacket();
        packet.setPacketId(++packetId);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        c.getSession2().multiStatementPacket(packet, packetId);
        return packet;
    }

}
