/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.parser.ServerParse;
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
        PhysicalDataNode mysqlDN = conf.getDataNodes().get(seqVal.dataNode);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute in data node " + seqVal.dataNode +
                        " for fetch sequence sql " + seqVal.sql);
            }
            // change Select mode to Update mode. Make sure the query send to the write host
            mysqlDN.getConnection(mysqlDN.getDatabase(), true, true,
                    new RouteResultsetNode(seqVal.dataNode, ServerParse.UPDATE,
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
        conn.setResponseHandler(this);
        ((MySQLConnection) conn).setComplexQuery(true);
        try {
            conn.query(((SequenceVal) conn.getAttachment()).sql, true);
        } catch (Exception e) {
            LOGGER.warn("connection acquired error: " + e);
            handleError(conn, e.getMessage());
            conn.close(e.getMessage());
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("connect error: " + e);
        handleError(conn, e.getMessage());
        conn.closeWithoutRsp(e.getMessage());
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String errMsg = new String(err.getMessage());

        LOGGER.warn("errorResponse " + err.getErrNo() + " " + errMsg);
        handleError(conn, errMsg);

        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            conn.release();
        } else {
            conn.closeWithoutRsp("unfinished sync");
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            ((SequenceVal) conn.getAttachment()).dbfinished = true;
            conn.release();
        }

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        RowDataPacket rowDataPkg = new RowDataPacket(1);
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(0);
        String columnVal = new String(columnData);
        SequenceVal seqVal = (SequenceVal) conn.getAttachment();
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
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        ((SequenceVal) conn.getAttachment()).dbfinished = true;
        conn.release();
    }

    private void handleError(BackendConnection c, String errMsg) {
        SequenceVal seqVal = ((SequenceVal) c.getAttachment());
        IncrSequenceMySQLHandler.LATEST_ERRORS.put(seqVal.seqName, errMsg);
        seqVal.dbretVal = null;
        seqVal.dbfinished = true;
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn("connection " + conn + " closed, reason:" + reason);
        handleError(conn, "connection " + conn + " closed, reason:" + reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {

    }

}
