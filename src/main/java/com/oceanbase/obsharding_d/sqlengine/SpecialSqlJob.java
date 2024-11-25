/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.sqlengine;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.config.ErrorInfo;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by szf on 2018/9/20.
 */
public class SpecialSqlJob extends SQLJob {

    public static final Logger LOGGER = LoggerFactory.getLogger(SpecialSqlJob.class);

    private final SQLJobHandler jobHandler;
    private final ResponseHandler sqlJob;
    private final PhysicalDbInstance ds;
    private final String schema;
    private final String sql;
    private final List<ErrorInfo> list;
    private final AtomicBoolean finished;

    public SpecialSqlJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDbInstance ds, List<ErrorInfo> list) {
        super(sql, schema, jobHandler, ds);
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.sql = sql;
        this.list = list;
        this.sqlJob = this;
        this.finished = new AtomicBoolean(false);
    }


    public void run() {
        //create new connection and exec the sql
        OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    ds.createConnectionSkipPool(schema, sqlJob);
                } catch (Exception e) {
                    sqlJob.connectionError(e, null);
                }
            }
        });
    }

    @Override
    protected boolean doFinished(boolean failed) {
        if (finished.compareAndSet(false, true)) {
            jobHandler.finished(schema, failed);
            return true;
        }
        return false;
    }


    @Override
    public void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("can't get connection for sql :" + sql, e);
        list.add(new ErrorInfo("Meta", "WARNING", "Can't get connection for Meta check in shardingNode[" + ds.getName() + "." + schema + "]"));
        doFinished(true);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        try {
            conn.getBackendService().query(sql, true);
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            doFinished(true);
        }
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + service;

        list.add(new ErrorInfo("Meta", "WARNING", "Execute show tables in shardingNode[" + ds.getName() + "." + schema + "] get error"));

        LOGGER.info(errMsg);
        doFinished(true);
        service.getConnection().close("dry run error backend");
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        if (doFinished(true)) {
            list.add(new ErrorInfo("Meta", "WARNING", "Execute show tables in shardingNode[" + ds.getName() + "." + schema + "] get connection closed"));
        }
    }


    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        doFinished(false);
        service.getConnection().close("dry run used connection");
    }

}
