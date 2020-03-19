/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TransformSQLJob implements ResponseHandler, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransformSQLJob.class);
    private final String sql;
    private final String databaseName;
    private final PhysicalDataSource ds;
    private final ManagerConnection mc;
    private BackendConnection connection;

    public TransformSQLJob(String sql, String databaseName, PhysicalDataSource ds, ManagerConnection mc) {
        this.sql = sql;
        this.databaseName = databaseName;
        this.ds = ds;
        this.mc = mc;
    }

    @Override
    public void run() {
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(databaseName, ServerParse.SELECT, sql);
                // create new connection
                PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), false, true, node, this, node);
            } else {
                ds.getConnection(databaseName, true, this, null, false);
            }
        } catch (Exception e) {
            LOGGER.warn("can't get connection", e);
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setPacketId(1);
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
            writeError(errPacket.toBytes());
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("can't get connection for sql :" + sql, e);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_YES);
        errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
        writeError(errPacket.toBytes());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.setResponseHandler(this);
        connection = conn;
        try {
            ((MySQLConnection) conn).sendQueryCmd(sql, mc.getCharset());
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setPacketId(1);
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
            writeError(errPacket.toBytes());
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        writeError(err);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        mc.write(ok);
        connection.release();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        mc.write(header);
        for (byte[] field : fields) {
            mc.write(field);
        }
        mc.write(eof);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        mc.write(row);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        mc.write(eof);
        connection.release();
    }

    @Override
    public void writeQueueAvailable() {
        //do nothing
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_YES);
        errPacket.setMessage(StringUtil.encode(reason, StandardCharsets.UTF_8.toString()));
        writeError(errPacket.toBytes());
    }

    private void writeError(byte[] err) {
        mc.write(err);
        if (connection != null) {
            connection.release();
        }
    }
}
