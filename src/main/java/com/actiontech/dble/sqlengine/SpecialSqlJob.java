package com.actiontech.dble.sqlengine;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.DelegateResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.ErrorInfo;
import com.actiontech.dble.net.mysql.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Created by szf on 2018/9/20.
 */
public class SpecialSqlJob extends SQLJob {

    public static final Logger LOGGER = LoggerFactory.getLogger(SpecialSqlJob.class);

    private final SQLJobHandler jobHandler;
    private final ResponseHandler sqlJob;
    private final PhysicalDatasource ds;
    private final String schema;
    private final String sql;
    private final List<ErrorInfo> list;
    private volatile boolean finished;

    public SpecialSqlJob(String sql, String schema, SQLJobHandler jobHandler, PhysicalDatasource ds, List<ErrorInfo> list) {
        super(sql, schema, jobHandler, ds);
        this.jobHandler = jobHandler;
        this.ds = ds;
        this.schema = schema;
        this.sql = sql;
        this.list = list;
        this.sqlJob = this;
    }


    public void run() {
        try {
            //create new connection and exec the sql
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ds.createNewConnection(new DelegateResponseHandler(sqlJob), schema);
                    } catch (Exception e) {
                        sqlJob.connectionError(e, null);
                    }
                }
            });

        } catch (Exception e) {
            LOGGER.info("can't get connection for sql ,error:" + e);
            doFinished(true);
        }
    }


    private void doFinished(boolean failed) {
        finished = true;
        jobHandler.finished(schema, failed);
    }


    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("can't get connection for sql :" + sql, e);
        list.add(new ErrorInfo("Meta", "WARNING", "Can't get connection for Meta check in dataNode[" + ds.getName() + "." + schema + "]"));
        doFinished(true);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        conn.setResponseHandler(this);
        ((MySQLConnection) conn).setComplexQuery(true);
        try {
            conn.query(sql);
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
        conn.close("dry run error backend");
        doFinished(true);
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        list.add(new ErrorInfo("Meta", "WARNING", "Execute show tables in dataNode[" + ds.getName() + "." + schema + "] get connection closed"));
        doFinished(true);
    }

}
