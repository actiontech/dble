/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorInfo;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.service.AbstractService;
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
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
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
    public void errorResponse(byte[] err, AbstractService service) {
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
    public void connectionClose(AbstractService service, String reason) {
        if (doFinished(true)) {
            list.add(new ErrorInfo("Meta", "WARNING", "Execute show tables in shardingNode[" + ds.getName() + "." + schema + "] get connection closed"));
        }
    }


    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        doFinished(false);
        service.getConnection().close("dry run used connection");
    }

}
