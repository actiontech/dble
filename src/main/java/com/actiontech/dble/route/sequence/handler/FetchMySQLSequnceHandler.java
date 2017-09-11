/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
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
public class FetchMySQLSequnceHandler implements ResponseHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(FetchMySQLSequnceHandler.class);

    public void execute(SequenceVal seqVal) {
        ServerConfig conf = DbleServer.getInstance().getConfig();
        PhysicalDBNode mysqlDN = conf.getDataNodes().get(seqVal.dataNode);
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute in datanode " + seqVal.dataNode +
                        " for fetch sequnce sql " + seqVal.sql);
            }
            // change Select mode to Update mode. Make sure the query send to the write host
            mysqlDN.getConnection(mysqlDN.getDatabase(), true,
                    new RouteResultsetNode(seqVal.dataNode, ServerParse.UPDATE,
                            seqVal.sql), this, seqVal);
        } catch (Exception e) {
            LOGGER.warn("get connection err " + e);
        }

    }

    public String getLastestError(String seqName) {
        return IncrSequenceMySQLHandler.LATEST_ERRORS.get(seqName);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {

        conn.setResponseHandler(this);
        try {
            conn.query(((SequenceVal) conn.getAttachment()).sql);
        } catch (Exception e) {
            executeException(conn, e);
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        ((SequenceVal) conn.getAttachment()).dbfinished = true;
        LOGGER.warn("connectionError " + e);

    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        SequenceVal seqVal = ((SequenceVal) conn.getAttachment());
        seqVal.dbfinished = true;

        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String errMsg = new String(err.getMessage());
        LOGGER.warn("errorResponse " + err.getErrno() + " " + errMsg);
        IncrSequenceMySQLHandler.LATEST_ERRORS.put(seqVal.seqName, errMsg);
        conn.release();

    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExcute();
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
            LOGGER.warn(" sequnce sql returned err value ,sequence:" +
                    seqVal.seqName + " " + columnVal + " sql:" + seqVal.sql);
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

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {

    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {

    }

    private void executeException(BackendConnection c, Throwable e) {
        SequenceVal seqVal = ((SequenceVal) c.getAttachment());
        seqVal.dbfinished = true;
        String errMgs = e.toString();
        IncrSequenceMySQLHandler.LATEST_ERRORS.put(seqVal.seqName, errMgs);
        LOGGER.warn("executeException   " + errMgs);
        c.close("exception:" + errMgs);

    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {

        LOGGER.warn("connection closed " + conn + " reason:" + reason);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {

    }

}
