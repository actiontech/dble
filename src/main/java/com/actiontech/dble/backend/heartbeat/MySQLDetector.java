/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.sqlengine.HeartbeatSQLJob;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mycat
 */
public class MySQLDetector implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLDetector.class);
    private MySQLHeartbeat heartbeat;

    private final AtomicBoolean isQuit;
    private volatile long lastSendQryTime;
    private volatile long lastReceivedQryTime;
    private volatile HeartbeatSQLJob sqlJob;
    private BackendConnection con;

    private static final String[] MYSQL_SLAVE_STATUS_COLS = new String[]{
            "Seconds_Behind_Master",
            "Slave_IO_Running",
            "Slave_SQL_Running",
            "Slave_IO_State",
            "Master_Host",
            "Master_User",
            "Master_Port",
            "Connect_Retry",
            "Last_IO_Error"};

    private static final String[] MYSQL_CLUSTER_STATUS_COLS = new String[]{
            "Variable_name",
            "Value"};

    public MySQLDetector(MySQLHeartbeat heartbeat) {
        this(heartbeat, 0L);
    }

    public MySQLDetector(MySQLHeartbeat heartbeat, long lastReceivedQryTime) {
        this.heartbeat = heartbeat;
        this.lastReceivedQryTime = lastReceivedQryTime;
        this.isQuit = new AtomicBoolean(false);
        con = null;
        try {
            MySQLDataSource ds = heartbeat.getSource();
            con = ds.getConnectionForHeartbeat(null, true);
        } catch (IOException e) {
            LOGGER.warn("heartbeat error", e);

        }
    }

    public boolean isHeartbeatTimeout() {
        return System.currentTimeMillis() > Math.max(lastSendQryTime, lastReceivedQryTime) + heartbeat.getHeartbeatTimeout();
    }


    public long getLastReceivedQryTime() {
        return lastReceivedQryTime;
    }

    public void heartbeat() {
        if (con == null || con.isClosed()) {
            heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
            return;
        }
        lastSendQryTime = System.currentTimeMillis();


        String[] fetchCols = {};
        if (heartbeat.getSource().getHostConfig().isShowSlaveSql()) {
            fetchCols = MYSQL_SLAVE_STATUS_COLS;
        }
        if (heartbeat.getSource().getHostConfig().isShowClusterSql()) {
            fetchCols = MYSQL_CLUSTER_STATUS_COLS;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("do heartbeat,conn is " + con);
        }
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(fetchCols, this);
        sqlJob = new HeartbeatSQLJob(heartbeat.getHeartbeatSQL(), con, resultHandler);
        sqlJob.execute();
    }

    public void quit() {
        if (isQuit.compareAndSet(false, true)) {
            close("heart beat quit");
        }

    }

    public boolean isQuit() {
        return isQuit.get();
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (result.isSuccess()) {
            PhysicalDatasource source = heartbeat.getSource();
            Map<String, String> resultResult = result.getResult();
            if (source.getHostConfig().isShowSlaveSql()) {
                setStatusBySlave(source, resultResult);
            } else if (source.getHostConfig().isShowClusterSql()) {
                setStatusByCluster(resultResult);
            } else {
                heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
            }
        } else {
            heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
        }
        lastReceivedQryTime = System.currentTimeMillis();
        heartbeat.getRecorder().set((lastReceivedQryTime - lastSendQryTime));
    }

    private void setStatusByCluster(Map<String, String> resultResult) {
        //String Variable_name = resultResult != null ? resultResult.get("Variable_name") : null;
        String wsrepClusterStatus = resultResult != null ? resultResult.get("wsrep_cluster_status") : null; // Primary
        String wsrepConnected = resultResult != null ? resultResult.get("wsrep_connected") : null; // ON
        String wsrepReady = resultResult != null ? resultResult.get("wsrep_ready") : null; // ON
        if ("ON".equals(wsrepConnected) && "ON".equals(wsrepReady) && "Primary".equals(wsrepClusterStatus)) {
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_NORMAL);
            heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
        } else {
            MySQLHeartbeat.LOGGER.warn("found MySQL  cluster status err !!! " +
                    heartbeat.getSource().getConfig() + " wsrep_cluster_status: " + wsrepClusterStatus +
                    " wsrep_connected: " + wsrepConnected + " wsrep_ready: " + wsrepReady
            );
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_ERROR);
            heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
        }
        heartbeat.getAsyncRecorder().setByCluster(resultResult);
    }

    private void setStatusBySlave(PhysicalDatasource source, Map<String, String> resultResult) {
        String slaveIoRunning = resultResult != null ? resultResult.get("Slave_IO_Running") : null;
        String slaveSqlRunning = resultResult != null ? resultResult.get("Slave_SQL_Running") : null;
        if (slaveIoRunning != null && slaveIoRunning.equals(slaveSqlRunning) && slaveSqlRunning.equals("Yes")) {
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_NORMAL);
            String secondsBehindMaster = resultResult.get("Seconds_Behind_Master");
            if (null != secondsBehindMaster && !"".equals(secondsBehindMaster) && !"NULL".equalsIgnoreCase(secondsBehindMaster)) {
                int behindMaster = Integer.parseInt(secondsBehindMaster);
                if (behindMaster > source.getHostConfig().getSlaveThreshold()) {
                    MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication delay !!! " + heartbeat.getSource().getConfig() + ", binlog sync time delay: " + behindMaster + "s");
                }
                heartbeat.setSlaveBehindMaster(behindMaster);
            } else {
                heartbeat.setSlaveBehindMaster(null);
            }
        } else if (source.isSalveOrRead()) {
            //String Last_IO_Error = resultResult != null ? resultResult.get("Last_IO_Error") : null;
            MySQLHeartbeat.LOGGER.warn("found MySQL master/slave Replication err !!! " +
                    heartbeat.getSource().getConfig() + ", " + resultResult);
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_ERROR);
            heartbeat.setSlaveBehindMaster(null);
        }
        heartbeat.getAsyncRecorder().setBySlaveStatus(resultResult);
        heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
    }

    public void close(String msg) {
        HeartbeatSQLJob curJob = sqlJob;
        if (curJob != null) {
            curJob.terminate(msg);
            sqlJob = null;
        }
    }
}
