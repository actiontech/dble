/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author digdeep@126.com
 */
public class MySQLConsistencyHelper implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConsistencyHelper.class);
    private MySQLConsistencyChecker heartbeat;
    private volatile SQLJob sqlJob;
    private int retryTimes = 5;
    private AtomicInteger retryTime = new AtomicInteger();

    public MySQLConsistencyHelper(MySQLConsistencyChecker heartbeat, SQLJob sqlJob) {
        this.heartbeat = heartbeat;
        this.sqlJob = sqlJob;
        this.retryTime.set(retryTimes);
    }

    public MySQLConsistencyHelper(MySQLConsistencyChecker heartbeat,
                                  SQLJob sqlJob, int retryTime) {
        this.heartbeat = heartbeat;
        this.sqlJob = sqlJob;
        if (retryTime > 0 && retryTime < 10)
            this.retryTime.set(retryTime);
        else
            this.retryTime.set(retryTimes);
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        LOGGER.debug("resultresultresultresult:" + JSON.toJSONString(result));
        Map<String, String> rowMap = null;
        String count = null;
        String innerCol = null;
        String maxTimestamp = null;
        if (result != null)
            rowMap = result.getResult();

        if (rowMap != null) {
            maxTimestamp = rowMap.get(GlobalTableUtil.MAX_COLUMN);
            count = rowMap.get(GlobalTableUtil.COUNT_COLUMN);
            innerCol = rowMap.get(GlobalTableUtil.INNER_COLUMN);
            if ((rowMap.containsKey(GlobalTableUtil.MAX_COLUMN) && StringUtils.isNotBlank(maxTimestamp)) ||
                    (rowMap.containsKey(GlobalTableUtil.COUNT_COLUMN) && StringUtils.isNotBlank(count)) ||
                    (rowMap.containsKey(GlobalTableUtil.INNER_COLUMN) && StringUtils.isNotBlank(innerCol))) {
                heartbeat.setResult(result);
                return;
            } else {
                if (this.retryTime.get() > 0) {
                    try {
                        TimeUnit.MICROSECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        //ignore error
                    }
                    this.retryTime.decrementAndGet();
                    this.sqlJob.run();
                    return;
                }
                heartbeat.setResult(result);
                return;
            }
        } else {
            if (this.retryTime.get() > 0) {
                try {
                    TimeUnit.MICROSECONDS.sleep(3);
                } catch (InterruptedException e) {
                    //ignore error
                }
                this.retryTime.decrementAndGet();
                this.sqlJob.run();
                return;
            }
            heartbeat.setResult(result);
            return;
        }
    }

    public void close(String msg) {
        SQLJob curJob = sqlJob;
        if (curJob != null && !curJob.isFinished()) {
            curJob.teminate(msg);
            sqlJob = null;
        }
    }

    public void setSqlJob(SQLJob sqlJob) {
        this.sqlJob = sqlJob;
    }

}
