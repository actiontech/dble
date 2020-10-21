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
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResetConnectionPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SetTestJob implements ResponseHandler, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(SQLJob.class);

    private final String sql;
    private final SQLJobHandler jobHandler;
    private final BusinessService frontService;
    private final MysqlVariable[] setItems;
    private volatile List<FieldPacket> fieldPackets;
    private final int userVariableSize;
    private final AtomicBoolean hasReturn;

    public SetTestJob(String sql, SQLJobHandler jobHandler, MysqlVariable[] setItems, int userVariableSize, BusinessService frontService) {

        this.sql = sql;
        this.jobHandler = jobHandler;
        this.frontService = frontService;
        this.setItems = setItems;
        this.userVariableSize = userVariableSize;
        this.fieldPackets = new ArrayList<>(userVariableSize);
        this.hasReturn = new AtomicBoolean(false);
    }

    public void run() {
        boolean sendTest = false;
        try {

            if (frontService instanceof ShardingService) {
                Map<String, PhysicalDbGroup> dbGroups = DbleServer.getInstance().getConfig().getDbGroups();
                for (PhysicalDbGroup dbGroup : dbGroups.values()) {
                    if (dbGroup.getWriteDbInstance().isAlive()) {
                        dbGroup.getWriteDbInstance().getConnection(null, this, null, false);
                        sendTest = true;
                        break;
                    }
                }
            } else {
                ((RWSplitService) frontService).getSession().getRwGroup().getWriteDbInstance().getConnection(null, this, null, false);
                sendTest = true;
            }

        } catch (Exception e) {
            if (hasReturn.compareAndSet(false, true)) {
                String reason = "can't get backend connection for sql :" + sql + " " + e.getMessage();
                LOGGER.warn(reason, e);
                doFinished(true);
                frontService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
            }
        }
        if (!sendTest && hasReturn.compareAndSet(false, true)) {
            String reason = "can't get backend connection for sql[" + sql + "],because all dbInstances are dead.";
            LOGGER.warn(reason);
            doFinished(true);
            frontService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        conn.getBackendService().execute(frontService, sql);
    }

    private void doFinished(boolean failed) {
        jobHandler.finished(null, failed);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        if (hasReturn.compareAndSet(false, true)) {
            String reason = "can't get backend connection for sql :" + sql + " " + e.getMessage();
            LOGGER.info(reason);
            doFinished(true);
            frontService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, reason);
        }
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        if (hasReturn.compareAndSet(false, true)) {
            LOGGER.info("connectionClose sql :" + sql);
            doFinished(true);
            this.frontService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "connectionClose:" + reason);
        }
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        if (hasReturn.compareAndSet(false, true)) {
            ErrorPacket errPg = new ErrorPacket();
            errPg.read(err);
            doFinished(true);
            ((MySQLResponseService) service).release(); //conn context not change
            this.frontService.writeErrMessage(errPg.getErrNo(), new String(errPg.getMessage()));
        }
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        if (!responseService.syncAndExecute()) {
            return;
        }

        if (userVariableSize > 0) {
            return;
        }

        if (hasReturn.compareAndSet(false, true)) {
            doFinished(false);
            ResetConnHandler handler = new ResetConnHandler();
            responseService.setResponseHandler(handler);
            responseService.setComplexQuery(true);
            responseService.writeDirectly(responseService.writeToBuffer(ResetConnectionPacket.RESET, responseService.allocate()));
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fps, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        for (byte[] field : fields) {
            // save field
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            fieldPackets.add(fieldPk);
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        RowDataPacket rowDataPk = new RowDataPacket(fieldPackets.size());
        rowDataPk.read(row);
        for (int i = 0; i < userVariableSize; i++) {
            if (rowDataPk.getValue(i) == null) {
                continue;
            }
            int type = fieldPackets.get(i).getType();
            if (type == Fields.FIELD_TYPE_LONG || type == Fields.FIELD_TYPE_LONGLONG || type == Fields.FIELD_TYPE_NEW_DECIMAL ||
                    type == Fields.FIELD_TYPE_FLOAT | type == Fields.FIELD_TYPE_DOUBLE) {
                setItems[i].setValue(new String(rowDataPk.getValue(i)));
            } else {
                setItems[i].setValue("'" + new String(rowDataPk.getValue(i)) + "'");
            }
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        if (hasReturn.compareAndSet(false, true)) {
            doFinished(false);
            ResetConnHandler handler = new ResetConnHandler();
            responseService.setResponseHandler(handler);
            responseService.setComplexQuery(true);
            responseService.writeDirectly(responseService.writeToBuffer(ResetConnectionPacket.RESET, responseService.allocate()));
        }
    }

    @Override
    public String toString() {
        return "SQLJob [sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }
}
