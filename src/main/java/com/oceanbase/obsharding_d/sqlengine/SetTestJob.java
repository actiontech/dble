/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.sqlengine;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResetConnHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.ResetConnectionPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.plan.common.field.FieldUtil;
import com.oceanbase.obsharding_d.server.variables.MysqlVariable;
import com.oceanbase.obsharding_d.services.BusinessService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import com.oceanbase.obsharding_d.util.HexFormatUtil;
import org.jetbrains.annotations.NotNull;
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
                Map<String, PhysicalDbGroup> dbGroups = OBsharding_DServer.getInstance().getConfig().getDbGroups();
                for (PhysicalDbGroup dbGroup : dbGroups.values()) {
                    if (dbGroup.getWriteDbInstance().isAlive()) {
                        dbGroup.getWriteDbInstance().getConnection(null, this, null, false);
                        sendTest = true;
                        break;
                    }
                }
            } else {
                ((RWSplitService) frontService).getSession2().getRwGroup().getWriteDbInstance().getConnection(null, this, null, false);
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
    public void connectionClose(@NotNull AbstractService service, String reason) {
        if (hasReturn.compareAndSet(false, true)) {
            LOGGER.info("connectionClose sql :" + sql);
            doFinished(true);
            this.frontService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "connectionClose:" + reason);
        }
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        if (hasReturn.compareAndSet(false, true)) {
            ErrorPacket errPg = new ErrorPacket();
            errPg.read(err);
            doFinished(true);
            ((MySQLResponseService) service).release(); //conn context not change
            this.frontService.writeErrMessage(errPg.getErrNo(), new String(errPg.getMessage()));
        }
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
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
            responseService.write(ResetConnectionPacket.RESET, WriteFlags.QUERY_END, ResultFlag.OK);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fps, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        for (byte[] field : fields) {
            // save field
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            fieldPackets.add(fieldPk);
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        RowDataPacket rowDataPk = new RowDataPacket(fieldPackets.size());
        rowDataPk.read(row);

        FieldPacket fieldPacket;
        for (int i = 0; i < userVariableSize; i++) {
            if (rowDataPk.getValue(i) == null) {
                continue;
            }
            fieldPacket = fieldPackets.get(i);
            if (FieldUtil.isNumberType(fieldPacket.getType())) {
                setItems[i].setValue(new String(rowDataPk.getValue(i)));
            } else if (FieldUtil.isBinaryType(fieldPacket.getType()) && (fieldPacket.getFlags() & FieldUtil.BINARY_FLAG) != 0) {
                setItems[i].setValue("0x" + HexFormatUtil.bytesToHexString(rowDataPk.getValue(i)));
            } else {
                setItems[i].setValue("'" + new String(rowDataPk.getValue(i)) + "'");
            }
        }
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        if (hasReturn.compareAndSet(false, true)) {
            doFinished(false);
            ResetConnHandler handler = new ResetConnHandler();
            responseService.setResponseHandler(handler);
            responseService.setComplexQuery(true);
            responseService.write(ResetConnectionPacket.RESET, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
        }
    }

    @Override
    public String toString() {
        return "SQLJob [sql=" + sql + ",  jobHandler=" +
                jobHandler + "]";
    }
}
