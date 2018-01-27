/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author mycat
 */
public class KillConnectionHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillConnectionHandler.class);

    private final MySQLConnection toKilled;
    private final NonBlockingSession session;

    public KillConnectionHandler(BackendConnection toKilled,
                                 NonBlockingSession session) {
        this.toKilled = (MySQLConnection) toKilled;
        this.session = session;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        conn.setSession(session);
        CommandPacket packet = new CommandPacket();
        packet.setPacketId(0);
        packet.setCommand(MySQLPacket.COM_QUERY);
        packet.setArg(("KILL " + toKilled.getThreadId()).getBytes());
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        packet.write(mysqlCon);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        toKilled.close("exception:" + e.toString());
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("kill connection success connection id:" +
                    toKilled.getThreadId());
        }
        conn.release();
        toKilled.close("killed");

    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
        conn.quit();
        toKilled.close("killed");
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String msg = null;
        try {
            msg = new String(err.getMessage(), CharsetUtil.getJavaCharset(conn.getCharset().getResults()));
        } catch (UnsupportedEncodingException e) {
            msg = new String(err.getMessage());
        }
        LOGGER.info("kill backend connection " + toKilled + " failed: " + msg + " con:" + conn);
        conn.release();
        toKilled.close("exception:" + msg);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        return false;
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
    }
}
