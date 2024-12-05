/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.sequence.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.List;

/**
 * Created by huqing.yan on 2017/7/3.
 */
public class FetchMySQLSequenceHandler implements ResponseHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(FetchMySQLSequenceHandler.class);

    public void execute(SequenceVal seqVal) throws SQLNonTransientException {
        ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
        ShardingNode mysqlDN = conf.getShardingNodes().get(seqVal.shardingNode);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute in shardingNode " + seqVal.shardingNode +
                        " for fetch sequence sql " + seqVal.sql);
            }
            // change Select mode to Update mode. Make sure the query send to the writeDirectly host
            mysqlDN.getConnection(mysqlDN.getDatabase(), true, true,
                    new RouteResultsetNode(seqVal.shardingNode, ServerParse.UPDATE,
                            seqVal.sql), this, seqVal);
        } catch (Exception e) {
            LOGGER.warn("get connection err: " + e);
            throw new SQLNonTransientException(e.getMessage());
        }

    }

    String getLastError(String seqName) {
        return IncrSequenceMySQLHandler.LATEST_ERRORS.get(seqName);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        try {
            conn.getBackendService().query(((SequenceVal) conn.getBackendService().getAttachment()).sql, true);
        } catch (Exception e) {
            LOGGER.warn("connection acquired error: " + e);
            handleError(conn.getBackendService().getAttachment(), e.getMessage());
            conn.close(e.getMessage());
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("connect error: " + e);
        handleError(attachment, e.getMessage());
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String errMsg = new String(err.getMessage());

        LOGGER.warn("errorResponse " + err.getErrNo() + " " + errMsg);
        handleError(((MySQLResponseService) service).getAttachment(), errMsg);

        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            ((MySQLResponseService) service).release();
        } else {
            ((MySQLResponseService) service).getConnection().businessClose("unfinished sync");
        }
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            ((SequenceVal) ((MySQLResponseService) service).getAttachment()).dbfinished = true;
            ((MySQLResponseService) service).release();
        }

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        RowDataPacket rowDataPkg = new RowDataPacket(1);
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(0);
        String columnVal = new String(columnData);
        SequenceVal seqVal = (SequenceVal) ((MySQLResponseService) service).getAttachment();
        if (IncrSequenceMySQLHandler.ERR_SEQ_RESULT.equals(columnVal)) {
            seqVal.dbretVal = IncrSequenceMySQLHandler.ERR_SEQ_RESULT;
            String errMsg = "sequence sql returned err value, sequence:" +
                    seqVal.seqName + " " + columnVal + " sql:" + seqVal.sql;
            LOGGER.warn(errMsg);
            IncrSequenceMySQLHandler.LATEST_ERRORS.put(seqVal.seqName, errMsg);
        } else {
            seqVal.dbretVal = columnVal;
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        ((SequenceVal) ((MySQLResponseService) service).getAttachment()).dbfinished = true;
        ((MySQLResponseService) service).release();
    }

    private void handleError(Object attachment, String errMsg) {
        SequenceVal seqVal = ((SequenceVal) attachment);
        IncrSequenceMySQLHandler.LATEST_ERRORS.put(seqVal.seqName, errMsg);
        seqVal.dbretVal = null;
        seqVal.dbfinished = true;
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        LOGGER.warn("connection " + service + " closed, reason:" + reason);
        handleError(((MySQLResponseService) service).getAttachment(), "connection " + service + " closed, reason:" + reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {

    }

}
