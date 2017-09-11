/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author digdeep@126.com
 */
public class MySQLConsistencyChecker {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLConsistencyChecker.class);
    private final MySQLDataSource source;
    private final ReentrantLock lock;
    private AtomicInteger jobCount = new AtomicInteger();
    private String countSQL;
    private String maxSQL;
    private String tableName;    // global table name
    private long beginTime;
    private String[] physicalSchemas;
    private String columnExistSQL = "select group_concat(COLUMN_NAME separator ',') as " +
            GlobalTableUtil.INNER_COLUMN + " from information_schema.columns where TABLE_NAME='"; //user' and TABLE_SCHEMA='db1';

    private List<SQLQueryResult<Map<String, String>>> list = new ArrayList<>();


    public MySQLConsistencyChecker(MySQLDataSource source, String[] physicalSchemas, String tableName) {
        this.source = source;
        this.physicalSchemas = physicalSchemas;
        this.lock = new ReentrantLock(false);
        this.tableName = tableName;
        this.countSQL = " select count(*) as " + GlobalTableUtil.COUNT_COLUMN + " from " + this.tableName;
        this.maxSQL = " select max(" + GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN + ") as " +
                GlobalTableUtil.MAX_COLUMN + " from " + this.tableName;
        this.columnExistSQL += this.tableName + "' ";
    }

    public void checkRecordCout() {
        // ["db3","db2","db1"]
        lock.lock();
        try {
            this.jobCount.set(0);
            beginTime = new Date().getTime();
            for (String dbName : physicalSchemas) {
                MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null);
                OneRawSQLQueryResultHandler resultHandler =
                        new OneRawSQLQueryResultHandler(new String[]{GlobalTableUtil.COUNT_COLUMN}, detector);
                SQLJob sqlJob = new SQLJob(this.getCountSQL(), dbName, resultHandler, source);
                detector.setSqlJob(sqlJob);
                sqlJob.run();
                this.jobCount.incrementAndGet();
            }
        } finally {
            lock.unlock();
        }
    }

    public void checkMaxTimeStamp() {
        // ["db3","db2","db1"]
        lock.lock();
        try {
            this.jobCount.set(0);
            beginTime = new Date().getTime();
            for (String dbName : physicalSchemas) {
                MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null);
                OneRawSQLQueryResultHandler resultHandler =
                        new OneRawSQLQueryResultHandler(new String[]{GlobalTableUtil.MAX_COLUMN}, detector);
                SQLJob sqlJob = new SQLJob(this.getMaxSQL(), dbName, resultHandler, source);
                detector.setSqlJob(sqlJob);
                sqlJob.run();
                this.jobCount.incrementAndGet();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * check inner column exist or not
     */
    public void checkInnerColumnExist() {
        // ["db3","db2","db1"]
        lock.lock();
        try {
            this.jobCount.set(0);
            beginTime = new Date().getTime();
            for (String dbName : physicalSchemas) {
                MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null, 1);
                OneRawSQLQueryResultHandler resultHandler =
                        new OneRawSQLQueryResultHandler(new String[]{GlobalTableUtil.INNER_COLUMN}, detector);
                String db = " and table_schema='" + dbName + "'";
                SQLJob sqlJob = new SQLJob(this.columnExistSQL + db, dbName, resultHandler, source);
                detector.setSqlJob(sqlJob); //table_schema='db1'
                LOGGER.debug(sqlJob.toString());
                sqlJob.run();
                this.jobCount.incrementAndGet();
            }
        } finally {
            lock.unlock();
        }
    }

    public void setResult(SQLQueryResult<Map<String, String>> result) {
        // LOGGER.debug("setResult::::::::::" + JSON.toJSONString(result));
        lock.lock();
        try {
            this.jobCount.decrementAndGet();
            if (result != null && result.isSuccess()) {
                result.setTableName(tableName);
                list.add(result);
            } else {
                if (result != null && result.getResult() != null) {
                    String sql = null;
                    if (result.getResult().containsKey(GlobalTableUtil.COUNT_COLUMN))
                        sql = this.getCountSQL();
                    if (result.getResult().containsKey(GlobalTableUtil.MAX_COLUMN))
                        sql = this.getMaxSQL();
                    if (result.getResult().containsKey(GlobalTableUtil.INNER_COLUMN))
                        sql = this.getColumnExistSQL();
                    LOGGER.warn(sql + " execute failed in db: " + result.getDataNode() +
                            " during global table consistency check task.");
                }
            }
            if (this.jobCount.get() <= 0 || isTimeOut()) {
                GlobalTableUtil.finished(list);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isTimeOut() {
        long duration = new Date().getTime() - this.beginTime;
        return TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS) > 1;
    }

    public String getCountSQL() {
        return countSQL;
    }

    public String getColumnExistSQL() {
        return columnExistSQL;
    }

    public String getMaxSQL() {
        return maxSQL;
    }
}
