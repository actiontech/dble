/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.net.mysql.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class OneTimeConnJob extends SQLJob {
    public static final Logger LOGGER = LoggerFactory.getLogger(OneTimeConnJob.class);

    private final SQLJobHandler jobHandler;
    private final PhysicalDatasource ds;
    private final String schema;
    private final String sql;
    private final AtomicBoolean finished;

    public OneTimeConnJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDatasource ds) {
        super(sql, schema, jobHandler, ds);
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.sql = sql;
        this.finished = new AtomicBoolean(false);
    }

    public void run() {
        try {
            ds.getConnection(schema, true, this, null, false);
        } catch (Exception e) {
            this.connectionError(e, null);
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
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        conn.closeWithoutRsp("conn used for once");
        doFinished(false);
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + conn;

        LOGGER.info(errMsg);
        doFinished(true);
        conn.closeWithoutRsp("close conn for reason:" + errMsg);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (conn.syncAndExecute()) {
            conn.closeWithoutRsp("conn used for once");
            doFinished(false);
        }
    }
}
