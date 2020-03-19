/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorInfo;
import com.actiontech.dble.net.mysql.ErrorPacket;
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
    private final PhysicalDataSource ds;
    private final String schema;
    private final String sql;
    private final List<ErrorInfo> list;
    private final AtomicBoolean finished;

    public SpecialSqlJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDataSource ds, List<ErrorInfo> list) {
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
        try {
            //create new connection and exec the sql
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ds.getConnection(schema, true, sqlJob, null, false);
                    } catch (Exception e) {
                        sqlJob.connectionError(e, null);
                    }
                }
            });

        } catch (Exception e) {
            LOGGER.warn("can't get connection", e);
            doFinished(true);
        }
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
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("can't get connection for sql :" + sql, e);
        list.add(new ErrorInfo("Meta", "WARNING", "Can't get connection for Meta check in dataNode[" + ds.getName() + "." + schema + "]"));
        doFinished(true);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        ((MySQLConnection) conn).setComplexQuery(true);
        try {
            conn.query(sql, true);
        } catch (Exception e) { // (UnsupportedEncodingException e) {
            doFinished(true);
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + conn;

        list.add(new ErrorInfo("Meta", "WARNING", "Execute show tables in dataNode[" + ds.getName() + "." + schema + "] get error"));

        LOGGER.info(errMsg);
        doFinished(true);
        conn.close("dry run error backend");
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        if (doFinished(true)) {
            list.add(new ErrorInfo("Meta", "WARNING", "Execute show tables in dataNode[" + ds.getName() + "." + schema + "] get connection closed"));
        }
    }


    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        doFinished(false);
        conn.close("dry run used connection");
    }

}
