/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class OneTimeConnJob extends SQLJob {
    public static final Logger LOGGER = LoggerFactory.getLogger(OneTimeConnJob.class);

    private final SQLJobHandler jobHandler;
    private final PhysicalDbInstance ds;
    private final String schema;
    private final String sql;
    private final AtomicBoolean finished;

    public OneTimeConnJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDbInstance ds) {
        super(sql, schema, jobHandler, ds);
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.sql = sql;
        this.finished = new AtomicBoolean(false);
    }

    public void run() {
        try {
            ds.createConnectionSkipPool(schema, this);
        } catch (Exception e) {
            this.connectionError(e, null);
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setComplexQuery(true);
        try {
            conn.getBackendService().query(sql);
        } catch (Exception e) {
            e.printStackTrace();
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
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        service.getConnection().businessClose("conn used for once");
        doFinished(false);
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errNo:" + errPg.getErrNo() + ", " + new String(errPg.getMessage()) +
                " from of sql :" + sql + " at con:" + service;

        LOGGER.info(errMsg);
        doFinished(true);
        service.getConnection().businessClose("close conn for reason:" + errMsg);
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (((MySQLResponseService) service).syncAndExecute()) {
            service.getConnection().businessClose("conn used for once");
            doFinished(false);
        }
    }
}
