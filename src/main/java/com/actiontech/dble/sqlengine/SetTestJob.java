/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResetConnHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResetConnectionPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SetTestJob implements ResponseHandler, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);

    private final String sql;
    private final String dataNodeOrDatabase;
    private final SQLJobHandler jobHandler;
    private final ServerConnection sc;

    public SetTestJob(String sql, SQLJobHandler jobHandler, ServerConnection sc) {
        super();
        this.sql = sql;
        String schema = sc.getSchema();
        if (schema == null) {
            schema = SchemaUtil.getRandomDb();
        }
        this.dataNodeOrDatabase = schema;
        this.jobHandler = jobHandler;
        this.sc = sc;
    }

    public void run() {
        try {
            Map<String, PhysicalDBPool> dataHosts = DbleServer.getInstance().getConfig().getDataHosts();
            for (PhysicalDBPool dn : dataHosts.values()) {
                dn.getSource().getConnection(dataNodeOrDatabase, true, this, null);
                break;
            }
        } catch (Exception e) {
            String reason = "can't get backend connection for sql :" + sql;
            LOGGER.warn(reason, e);
            sc.close(reason);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.setResponseHandler(this);
        ((MySQLConnection) conn).sendQueryCmd(sql, sc.getCharset());
    }

    private void doFinished(boolean failed) {
        jobHandler.finished(dataNodeOrDatabase, failed);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        String reason = "can't get backend connection for sql :" + sql;
        LOGGER.warn(reason);
        sc.close(reason);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        LOGGER.warn("connectionClose sql :" + sql);
        sc.close(reason);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        doFinished(true);
        conn.release(); //conn context not change
        sc.writeErrMessage(errPg.getErrno(), new String(errPg.getMessage()));
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        doFinished(false);
        sc.write(ok);
        ResetConnHandler handler = new ResetConnHandler();
        conn.setResponseHandler(handler);
        MySQLConnection connection = (MySQLConnection) conn;
        connection.write(connection.writeToBuffer(ResetConnectionPacket.RESET, connection.allocate()));
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        //will not happen

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        //will not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        //will not happen
    }

    @Override
    public void writeQueueAvailable() {
    }

    @Override
    public String toString() {
        return "SQLJob [dataNodeOrDatabase=" +
                dataNodeOrDatabase + ",sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
    }
}
