/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.sqlengine;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.jetbrains.annotations.NotNull;
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
                ShardingNode dn = OBsharding_DServer.getInstance().getConfig().getShardingNodes().get(node.getName());
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
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        writeError(err);
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        this.managerService.write(ok, WriteFlags.QUERY_END, ResultFlag.OK);
        connection.release();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        managerService.write(header, WriteFlags.PART);
        for (byte[] field : fields) {
            managerService.write(field, WriteFlags.PART);
        }
        managerService.write(eof, WriteFlags.PART);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        managerService.write(row, WriteFlags.PART);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        managerService.write(eof, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
        connection.release();
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_YES);
        errPacket.setMessage(StringUtil.encode(reason, StandardCharsets.UTF_8.toString()));
        writeError(errPacket.toBytes());
    }

    private void writeError(byte[] err) {
        managerService.write(err, WriteFlags.SESSION_END, ResultFlag.ERROR);
        if (connection != null) {
            connection.release();
        }
    }
}
