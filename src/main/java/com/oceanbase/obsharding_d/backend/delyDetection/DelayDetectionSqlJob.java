/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.delyDetection;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.sqlengine.OneRawSQLQueryResultHandler;
import com.oceanbase.obsharding_d.sqlengine.SQLJobHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class DelayDetectionSqlJob implements ResponseHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(DelayDetectionSqlJob.class);

    private final DelayDetection delayDetection;
    private final SQLJobHandler jobHandler;
    private String sql;
    private long versionVal;
    private AtomicBoolean finished = new AtomicBoolean(false);
    private LocalDateTime responseTime;
    private long keepAlive = 60000;

    public DelayDetectionSqlJob(DelayDetection delayDetection, OneRawSQLQueryResultHandler jobHandler) {
        this.delayDetection = delayDetection;
        this.jobHandler = jobHandler;
        int delayPeriodMillis = delayDetection.getSource().getDbGroupConfig().getDelayPeriodMillis();
        this.keepAlive += delayPeriodMillis;
        sql = delayDetection.getSelectSQL();
        updateResponseTime();
    }

    public void execute() {
        updateResponseTime();
        finished.set(false);
        if (delayDetection.getSource().isReadInstance()) {
            sql = delayDetection.getSelectSQL();
        } else if (!delayDetection.isTableExists()) {
            sql = delayDetection.getCreateTableSQL();
        } else {
            sql = delayDetection.getUpdateSQL();
        }
        BackendConnection conn = delayDetection.getConn();
        if (Objects.isNull(conn)) {
            LOGGER.warn("[delayDetection]connection establishment timeout,please pay attention to network latency or packet loss.");
            delayDetection.cancel("connection establishment timeout");
            doFinished(true);
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[delayDetection]do delayDetection,conn is " + conn);
        }
        try {
            LocalDateTime now = LocalDateTime.now();
            Duration duration = Duration.between(responseTime, now);
            if (duration.toMillis() > keepAlive) {
                LOGGER.warn("[delayDetection]connection execution timeout {},please pay attention to network latency or packet loss.", duration.toMillis());
                delayDetection.cancel("connection execution timeout");
                doFinished(true);
            }
            conn.getBackendService().query(sql);
        } catch (Exception e) {
            LOGGER.warn("[delayDetection]send delayDetection error", e);
            delayDetection.cancel("send delayDetection error, because of [" + e.getMessage() + "]");
            doFinished(true);
        }
    }

    public void setVersionVal(long versionVal) {
        this.versionVal = versionVal;
    }

    private void doFinished(boolean failed) {
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(null, failed);
        }
    }

    private void updateResponseTime() {
        responseTime = LocalDateTime.now();
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("[delayDetection]can't get connection for sql :" + sql, e);
        updateResponseTime();
        delayDetection.cancel("delayDetection connection Error");
        doFinished(true);
    }

    @Override
    public void connectionAcquired(BackendConnection connection) {
        if (delayDetection.getVersion().get() == versionVal) {
            updateResponseTime();
            connection.getBackendService().setResponseHandler(this);
            connection.getBackendService().setComplexQuery(true);
            delayDetection.setConn(connection);
            execute();
        }
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);
        MySQLResponseService responseService = (MySQLResponseService) service;
        LOGGER.warn("[delayDetection]error response errNo: {}, {} from of sql: {} at con: {} db user = {}",
                errPg.getErrNo(), new String(errPg.getMessage()), sql, service,
                responseService.getConnection().getInstance().getConfig().getUser());
        updateResponseTime();
        delayDetection.cancel(new String(errPg.getMessage()));
        if (!((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("[delayDetection]unfinished sync");
            doFinished(true);
            return;
        }
        doFinished(true);
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        updateResponseTime();
        if (((MySQLResponseService) service).syncAndExecute()) {
            doFinished(false);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        updateResponseTime();
        jobHandler.onHeader(fields);
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        updateResponseTime();
        jobHandler.onRowData(row);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        updateResponseTime();
        doFinished(false);
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        updateResponseTime();
        LOGGER.warn("[delayDetection]conn for sql[" + sql + "] is closed, due to " + reason + ", we will try again immediately");
        delayDetection.cancel("delayDetection conn for sql[" + sql + "] is closed, due to " + reason);
        delayDetection.delayDetectionRetry();
        doFinished(true);
    }


}
