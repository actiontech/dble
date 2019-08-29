/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author mycat
 */
public class KillHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillHandler.class);

    private final NonBlockingSession session;
    private final boolean isKillQuery;
    private String cmd;

    public KillHandler(boolean isKillQuery, NonBlockingSession session) {
        this.isKillQuery = isKillQuery;
        if (isKillQuery) {
            cmd = "kill query %s";
        } else {
            cmd = "kill %s";
        }
        this.session = session;
    }

    public void execute() throws Exception {
        if (session.getTargetCount() <= 0) {
            return;
        }

        for (final RouteResultsetNode node : session.getTargetKeys()) {
            PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), session.getSource().isAutocommit(), node, this, node);
        }
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        conn.setSession(session);
        MySQLConnection killConn = (MySQLConnection) session.getTarget((RouteResultsetNode) conn.getAttachment());
        ((MySQLConnection) conn).execCmd(cmd.replace("%s", "" + killConn.getThreadId()));
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("connect error");
        if (conn != null && !isKillQuery) {
            MySQLConnection killConn = (MySQLConnection) session.getTarget((RouteResultsetNode) conn.getAttachment());
            AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + conn.toString() + " failed:" + e.getMessage(), null);
            killConn.close("exception:" + e.toString());
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        conn.release();
        if (!isKillQuery) {
            MySQLConnection killConn = (MySQLConnection) session.getTarget((RouteResultsetNode) conn.getAttachment());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("kill connection success connection id:" + killConn.getThreadId());
            }
            killConn.close("killed");
        }
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        MySQLConnection killConn = (MySQLConnection) session.getTarget((RouteResultsetNode) conn.getAttachment());
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String msg;
        try {
            msg = new String(err.getMessage(), CharsetUtil.getJavaCharset(conn.getCharset().getResults()));
        } catch (UnsupportedEncodingException e) {
            msg = new String(err.getMessage());
        }
        conn.release();
        if (!isKillQuery) {
            LOGGER.info("kill backend connection " + killConn + " failed: " + msg + " con:" + conn);
            AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + conn.toString() + " failed: " + msg, null);
            killConn.close("exception:" + msg);
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (!isKillQuery) {
            MySQLConnection killConn = (MySQLConnection) session.getTarget((RouteResultsetNode) conn.getAttachment());
            AlertUtil.alertSelf(AlarmCode.KILL_BACKEND_CONN_FAIL, Alert.AlertLevel.NOTICE, "get killer connection " + conn.toString() + " failed: connectionClosed", null);
            killConn.close("exception:" + reason);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected packet field eof response in kill handler");
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected packet row response in kill handler");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected packet row eof response in kill handler");
    }

    @Override
    public void writeQueueAvailable() {
    }

}
