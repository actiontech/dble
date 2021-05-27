/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.WriteFlags;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TransformSQLJob implements ResponseHandler, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransformSQLJob.class);
    private final String sql;
    private final String databaseName;
    private final PhysicalDbInstance ds;
    private final ManagerService managerService;
    private BackendConnection connection;

    public TransformSQLJob(String sql, String databaseName, PhysicalDbInstance ds, ManagerService managerService) {
        this.sql = sql;
        this.databaseName = databaseName;
        this.ds = ds;
        this.managerService = managerService;
    }

    @Override
    public void run() {
        try {
            if (ds == null) {
                RouteResultsetNode node = new RouteResultsetNode(databaseName, ServerParse.SELECT, sql);
                // create new connection
                ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), false, true, node, this, node);
            } else {
                ds.getConnection(databaseName, this, null, false);
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
    public void connectionError(Throwable e, Object attachment) {
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
        conn.getBackendService().setResponseHandler(this);
        connection = conn;
        try {
            conn.getBackendService().sendQueryCmd(sql, managerService.getCharset());
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setPacketId(1);
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage(StringUtil.encode(e.toString(), StandardCharsets.UTF_8.toString()));
            writeError(errPacket.toBytes());
        }
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        writeError(err);
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        this.managerService.write(ok, WriteFlags.QUERY_END);
        connection.release();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        managerService.write(header, WriteFlags.PART);
        for (byte[] field : fields) {
            managerService.write(field, WriteFlags.PART);
        }
        managerService.write(eof, WriteFlags.PART);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        managerService.write(row, WriteFlags.PART);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        managerService.write(eof, WriteFlags.QUERY_END);
        connection.release();
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_YES);
        errPacket.setMessage(StringUtil.encode(reason, StandardCharsets.UTF_8.toString()));
        writeError(errPacket.toBytes());
    }

    private void writeError(byte[] err) {
        managerService.write(err, WriteFlags.SESSION_END);
        if (connection != null) {
            connection.release();
        }
    }
}
