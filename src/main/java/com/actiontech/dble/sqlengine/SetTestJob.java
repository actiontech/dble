/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.mysql.nio.handler.ResetConnHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResetConnectionPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.handler.SetCallBack;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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
    private final ShardingService shardingService;
    private final AtomicBoolean hasReturn = new AtomicBoolean(false);

    public SetTestJob(String sql, String databaseName, SQLJobHandler jobHandler, ShardingService service) {
        super();
        this.sql = sql;
        this.databaseName = databaseName;
        this.jobHandler = jobHandler;
        this.shardingService = service;
    }

    public void run() {
        boolean sendTest = false;
        try {
            Map<String, PhysicalDbGroup> dbGroups = DbleServer.getInstance().getConfig().getDbGroups();
            for (PhysicalDbGroup dbGroup : dbGroups.values()) {
                if (dbGroup.getWriteDbInstance().isAlive()) {
                    dbGroup.getWriteDbInstance().getConnection(databaseName, this, null, false);
                    sendTest = true;
                    break;
                }
            }
        } catch (Exception e) {
            if (hasReturn.compareAndSet(false, true)) {
                String reason = "can't get backend connection for sql :" + sql + " " + e.getMessage();
                LOGGER.info(reason, e);
                doFinished(true);
                shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
            }
        }
        if (!sendTest && hasReturn.compareAndSet(false, true)) {
            String reason = "can't get backend connection for sql :" + sql + " all datasrouce dead";
            LOGGER.info(reason);
            doFinished(true);
            shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        conn.getBackendService().sendQueryCmd(sql, shardingService.getCharset());
    }

    private void doFinished(boolean failed) {
        jobHandler.finished(databaseName, failed);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        if (hasReturn.compareAndSet(false, true)) {
            String reason = "can't get backend connection for sql :" + sql + " " + e.getMessage();
            LOGGER.info(reason);
            doFinished(true);
            shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
        }
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        if (hasReturn.compareAndSet(false, true)) {
            LOGGER.info("connectionClose sql :" + sql);
            doFinished(true);
            this.shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "connectionClose:" + reason);
        }
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        if (hasReturn.compareAndSet(false, true)) {
            ErrorPacket errPg = new ErrorPacket();
            errPg.read(err);
            doFinished(true);
            ((MySQLResponseService) service).release(); //conn context not change
            this.shardingService.writeErrMessage(errPg.getErrNo(), new String(errPg.getMessage()));
        }
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        if (hasReturn.compareAndSet(false, true)) {
            doFinished(false);
            if (!((SetCallBack) ((OneRawSQLQueryResultHandler) jobHandler).getCallback()).isBackToOtherThread()) {
                shardingService.write(shardingService.getSession2().getOKPacket());
            }
            ResetConnHandler handler = new ResetConnHandler();
            responseService.setResponseHandler(handler);
            responseService.setComplexQuery(true);
            responseService.writeDirectly(responseService.writeToBuffer(ResetConnectionPacket.RESET, responseService.allocate()));
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        //will not happen

    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        //will not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        //will not happen
    }

    @Override
    public String toString() {
        return "SQLJob [Database=" +
                databaseName + ",sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }
}
