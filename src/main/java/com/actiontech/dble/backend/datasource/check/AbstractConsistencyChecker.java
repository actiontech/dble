package com.actiontech.dble.backend.datasource.check;

import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.manager.response.CheckGlobalConsistency;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2019/12/24.
 */
public abstract class AbstractConsistencyChecker implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {

    List<SQLJob> sqlJobs = new ArrayList<>();
    private final AtomicInteger count = new AtomicInteger();
    protected volatile String tableName;
    protected volatile String schema;
    protected volatile CheckGlobalConsistency handler = null;
    private List<SQLQueryResult<List<Map<String, String>>>> results = Collections.synchronizedList(new ArrayList<>());
    private List<SQLQueryResult<List<Map<String, String>>>> errorList = Collections.synchronizedList(new ArrayList<>());

    void addCheckNode(String dbName, PhysicalDataNode dataNode) {
        MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(this.getFetchCols(), this);
        SQLJob sqlJob = new SQLJob(this.getCountSQL(dbName, tableName), dataNode.getName(), resultHandler, true);
        sqlJobs.add(sqlJob);
    }

    void startCheckTable() {
        count.set(sqlJobs.size());
        for (SQLJob sqlJob : sqlJobs) {
            sqlJob.run();
        }
    }


    @Override
    public void onResult(SQLQueryResult<List<Map<String, String>>> result) {
        if (result.isSuccess()) {
            results.add(result);
        } else {
            errorList.add(result);
        }
        if (count.decrementAndGet() <= 0) {
            checkResults();
        }
    }

    private void checkResults() {
        List<SQLQueryResult<List<Map<String, String>>>> distinctList = new ArrayList();
        if (results.size() > 0) {
            distinctList.add(results.get(0));
            for (SQLQueryResult<List<Map<String, String>>> result : results) {
                boolean hasSame = false;
                for (SQLQueryResult<List<Map<String, String>>> drs : distinctList) {
                    if (resultEquals(result, drs)) {
                        hasSame = true;
                        break;
                    }
                }
                if (!hasSame) {
                    distinctList.add(result);
                }
            }
        }

        if (distinctList.size() > 1) {
            failResponse(distinctList);
        } else {
            resultResponse(errorList);
        }

        if (handler != null) {
            handler.collectResult(schema, tableName, distinctList.size(), errorList.size());
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setHandler(CheckGlobalConsistency handler) {
        this.handler = handler;
    }

    public abstract String[] getFetchCols();

    public abstract String getCountSQL(String dbName, String tName);

    public abstract boolean resultEquals(SQLQueryResult<List<Map<String, String>>> or, SQLQueryResult<List<Map<String, String>>> cr);

    public abstract void failResponse(List<SQLQueryResult<List<Map<String, String>>>> res);

    public abstract void resultResponse(List<SQLQueryResult<List<Map<String, String>>>> elist);
}
