/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResetConnHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResetConnectionPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SetTestJob implements ResponseHandler, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);

    private final String sql;
    private final String databaseName;
    private final SQLJobHandler jobHandler;
    private final ServerConnection sc;
    private final AtomicBoolean hasReturn = new AtomicBoolean(false);

    public SetTestJob(String sql, String databaseName, SQLJobHandler jobHandler, ServerConnection sc) {
        super();
        this.sql = sql;
        this.databaseName = databaseName;
        this.jobHandler = jobHandler;
        this.sc = sc;
    }

    public void run() {
        boolean sendTest = false;
        try {
            Map<String, PhysicalDataHost> dataHosts = DbleServer.getInstance().getConfig().getDataHosts();
            for (PhysicalDataHost dn : dataHosts.values()) {
                if (dn.getWriteSource().isAlive()) {
                    dn.getWriteSource().getConnection(databaseName, true, this, null, false);
                    sendTest = true;
                    break;
                }
            }
        } catch (Exception e) {
            if (hasReturn.compareAndSet(false, true)) {
                String reason = "can't get backend connection for sql :" + sql + " " + e.getMessage();
                LOGGER.info(reason, e);
                doFinished(true);
                sc.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
            }
        }
        if (!sendTest && hasReturn.compareAndSet(false, true)) {
            String reason = "can't get backend connection for sql :" + sql + " all datasrouce dead";
            LOGGER.info(reason);
            doFinished(true);
            sc.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.setResponseHandler(this);
        ((MySQLConnection) conn).setComplexQuery(true);
        ((MySQLConnection) conn).sendQueryCmd(sql, sc.getCharset());
    }

    private void doFinished(boolean failed) {
        jobHandler.finished(databaseName, failed);
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        if (hasReturn.compareAndSet(false, true)) {
            String reason = "can't get backend connection for sql :" + sql + " " + e.getMessage();
            LOGGER.info(reason);
            doFinished(true);
            sc.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (hasReturn.compareAndSet(false, true)) {
            LOGGER.info("connectionClose sql :" + sql);
            doFinished(true);
            sc.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "connectionClose:" + reason);
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        if (hasReturn.compareAndSet(false, true)) {
            ErrorPacket errPg = new ErrorPacket();
            errPg.read(err);
            doFinished(true);
            conn.release(); //conn context not change
            sc.writeErrMessage(errPg.getErrNo(), new String(errPg.getMessage()));
        }
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (hasReturn.compareAndSet(false, true)) {
            doFinished(false);
            boolean multiStatementFlag = sc.getSession2().getIsMultiStatement().get();
            sc.write(sc.writeToBuffer(sc.getSession2().getOkByteArray(), sc.allocate()));
            sc.getSession2().multiStatementNextSql(multiStatementFlag);
            ResetConnHandler handler = new ResetConnHandler();
            conn.setResponseHandler(handler);
            ((MySQLConnection) conn).setComplexQuery(true);
            MySQLConnection connection = (MySQLConnection) conn;
            connection.write(connection.writeToBuffer(ResetConnectionPacket.RESET, connection.allocate()));
        }
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
                databaseName + ",sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }
}
