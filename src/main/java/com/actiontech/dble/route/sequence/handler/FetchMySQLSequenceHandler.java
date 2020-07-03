/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;

import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by huqing.yan on 2017/7/3.
 */
public class FetchMySQLSequenceHandler implements ResponseHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(FetchMySQLSequenceHandler.class);

    public void execute(SequenceVal seqVal) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
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
    public void errorResponse(byte[] data, AbstractService service) {
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
    public void okResponse(byte[] ok, AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            ((SequenceVal) ((MySQLResponseService) service).getAttachment()).dbfinished = true;
            ((MySQLResponseService) service).release();
        }

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
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
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
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
    public void connectionClose(AbstractService service, String reason) {
        LOGGER.warn("connection " + service + " closed, reason:" + reason);
        handleError(((MySQLResponseService) service).getAttachment(), "connection " + service + " closed, reason:" + reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, AbstractService service) {

    }

}
