/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.config.helper.GetAndSyncDataSourceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
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

    private static final String[] MYSQL_READ_ONLY_COLS = new String[]{
            "@@read_only"};

    public MySQLDetector(MySQLHeartbeat heartbeat) {
        this.heartbeat = heartbeat;
        this.isQuit = new AtomicBoolean(false);
        con = null;
        try {
            MySQLDataSource ds = heartbeat.getSource();
            con = ds.getConnectionForHeartbeat(null);
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
        } else if (heartbeat.getSource().getHostConfig().isSelectReadOnlySql()) {
            fetchCols = MYSQL_READ_ONLY_COLS;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("do heartbeat,conn is " + con);
        }
        OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(fetchCols, this);
        sqlJob = new HeartbeatSQLJob(heartbeat, con, resultHandler);
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
        lastReceivedQryTime = System.currentTimeMillis();
        heartbeat.getRecorder().set((lastReceivedQryTime - lastSendQryTime));
        if (result.isSuccess()) {
            PhysicalDatasource source = heartbeat.getSource();
            Map<String, String> resultResult = result.getResult();
            if (source.getHostConfig().isShowSlaveSql()) {
                setStatusBySlave(source, resultResult);
            } else if (source.getHostConfig().isSelectReadOnlySql()) {
                setStatusByReadOnly(source, resultResult);
            } else {
                setStatusForNormalHeartbeat(source);
            }
        } else {
            heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
        }
    }

    private void setStatusForNormalHeartbeat(PhysicalDatasource source) {
        if (!heartbeat.isStop()) {
            if (heartbeat.getStatus() == MySQLHeartbeat.OK_STATUS) { // ok->ok
                if (!heartbeat.getSource().isSalveOrRead() && source.isReadOnly()) { // writehost check read only status is back?
                    GetAndSyncDataSourceKeyVariables task = new GetAndSyncDataSourceKeyVariables(source);
                    KeyVariables variables = task.call();
                    if (variables != null) {
                        source.setReadOnly(variables.isReadOnly());
                    } else {
                        LOGGER.warn("GetAndSyncDataSourceKeyVariables failed, set heartbeat Error");
                        heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
                        return;
                    }
                }
            } else if (heartbeat.getStatus() != MySQLHeartbeat.TIMEOUT_STATUS) { //error/init ->ok
                try {
                    source.testConnection();
                } catch (Exception e) {
                    LOGGER.warn("testConnection failed, set heartbeat Error");
                    heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
                    return;
                }
                GetAndSyncDataSourceKeyVariables task = new GetAndSyncDataSourceKeyVariables(source);
                KeyVariables variables = task.call();
                if (variables == null || variables.isLowerCase() != DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    String url = con.getHost() + ":" + con.getPort();
                    Map<String, String> labels = AlertUtil.genSingleLabel("data_host", url);
                    String errMsg = variables == null ? "GetAndSyncDataSourceKeyVariables failed" : "this dataHost[=" + url + "]'s lower_case is wrong";
                    LOGGER.warn(errMsg + ", set heartbeat Error");
                    if (variables != null) {
                        AlertUtil.alert(AlarmCode.DATA_HOST_LOWER_CASE_ERROR, Alert.AlertLevel.WARN, errMsg, "mysql", this.heartbeat.getSource().getConfig().getId(), labels);
                        ToResolveContainer.DATA_HOST_LOWER_CASE_ERROR.add(con.getHost() + ":" + con.getPort());
                    }
                    heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
                    return;
                } else {
                    String url = con.getHost() + ":" + con.getPort();
                    if (ToResolveContainer.DATA_HOST_LOWER_CASE_ERROR.contains(url)) {
                        Map<String, String> labels = AlertUtil.genSingleLabel("data_host", url);
                        AlertUtil.alertResolve(AlarmCode.DATA_HOST_LOWER_CASE_ERROR, Alert.AlertLevel.WARN, "mysql", this.heartbeat.getSource().getConfig().getId(), labels,
                                ToResolveContainer.DATA_HOST_LOWER_CASE_ERROR, url);
                    }
                    if (!source.isSalveOrRead()) { // writehost check read only
                        source.setReadOnly(variables.isReadOnly());
                    }
                }
            }
        }
        heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
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

    private void setStatusByReadOnly(PhysicalDatasource source, Map<String, String> resultResult) {
        String readonly = resultResult != null ? resultResult.get("@@read_only") : null;
        if (readonly == null) {
            heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS);
            return;
        } else if (readonly.equals("0")) {
            source.setReadOnly(false);
        } else {
            source.setReadOnly(true);
        }
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
