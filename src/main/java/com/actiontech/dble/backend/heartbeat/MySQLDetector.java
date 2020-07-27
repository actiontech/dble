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
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.helper.GetAndSyncDbInstanceKeyVariables;
import com.actiontech.dble.config.helper.KeyVariables;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mycat
 */
public class MySQLDetector implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLDetector.class);
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
    private static final String[] MYSQL_READ_ONLY_COLS = new String[]{"@@read_only"};

    private final AtomicBoolean isQuit;
    private MySQLHeartbeat heartbeat;
    private volatile long lastSendQryTime;
    private volatile long lastReceivedQryTime;
    private volatile HeartbeatSQLJob sqlJob;

    public MySQLDetector(MySQLHeartbeat heartbeat) {
        this.heartbeat = heartbeat;
        this.isQuit = new AtomicBoolean(false);
    }

    boolean isHeartbeatTimeout() {
        return System.currentTimeMillis() > Math.max(lastSendQryTime, lastReceivedQryTime) + heartbeat.getHeartbeatTimeout();
    }

    long getLastReceivedQryTime() {
        return lastReceivedQryTime;
    }

    public void heartbeat() {
        lastSendQryTime = System.currentTimeMillis();
        if (sqlJob == null) {
            String[] fetchCols = {};
            if (heartbeat.getSource().getDbGroupConfig().isShowSlaveSql()) {
                fetchCols = MYSQL_SLAVE_STATUS_COLS;
            } else if (heartbeat.getSource().getDbGroupConfig().isSelectReadOnlySql()) {
                fetchCols = MYSQL_READ_ONLY_COLS;
            }

            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(fetchCols, this);
            sqlJob = new HeartbeatSQLJob(heartbeat, resultHandler);
            heartbeat.getSource().createConnectionSkipPool(null, sqlJob);
        } else {
            sqlJob.execute();
        }
    }

    public void quit() {
        if (isQuit.compareAndSet(false, true)) {
            close();
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
            PhysicalDbInstance source = heartbeat.getSource();
            Map<String, String> resultResult = result.getResult();
            if (source.getDbGroupConfig().isShowSlaveSql()) {
                setStatusBySlave(source, resultResult);
            } else if (source.getDbGroupConfig().isSelectReadOnlySql()) {
                setStatusByReadOnly(source, resultResult);
            } else {
                setStatusForNormalHeartbeat(source);
            }
        }
    }

    private void setStatusForNormalHeartbeat(PhysicalDbInstance source) {
        if (checkRecoverFail(source)) return;
        heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
    }

    /**
     * if recover failed, return true
     */
    private boolean checkRecoverFail(PhysicalDbInstance source) {
        if (heartbeat.isStop()) {
            return true;
        }
        if (heartbeat.getStatus() == MySQLHeartbeat.OK_STATUS) { // ok->ok
            if (!heartbeat.getSource().isSalveOrRead() && source.isReadOnly()) { // writehost checkRecoverFail read only status is back?
                GetAndSyncDbInstanceKeyVariables task = new GetAndSyncDbInstanceKeyVariables(source, true);
                KeyVariables variables = task.call();
                if (variables != null) {
                    source.setReadOnly(variables.isReadOnly());
                } else {
                    LOGGER.warn("GetAndSyncDbInstanceKeyVariables failed, set heartbeat Error");
                    heartbeat.setErrorResult("GetAndSyncDbInstanceKeyVariables failed");
                    return true;
                }
            }
        } else if (heartbeat.getStatus() != MySQLHeartbeat.TIMEOUT_STATUS) { //error/init ->ok
            try {
                source.testConnection();
            } catch (Exception e) {
                LOGGER.warn("testConnection failed, set heartbeat Error");
                heartbeat.setErrorResult("testConnection failed");
                return true;
            }
            GetAndSyncDbInstanceKeyVariables task = new GetAndSyncDbInstanceKeyVariables(source, true);
            KeyVariables variables = task.call();
            if (variables == null ||
                    variables.isLowerCase() != DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames() ||
                    variables.getMaxPacketSize() < SystemConfig.getInstance().getMaxPacketSize()) {
                String url = heartbeat.getSource().getConfig().getUrl();
                Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", url);
                String errMsg;
                if (variables == null) {
                    errMsg = "GetAndSyncDbInstanceKeyVariables failed";
                } else if (variables.isLowerCase() != DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    errMsg = "this dbInstance[=" + url + "]'s lower_case is wrong";
                } else {
                    errMsg = "this dbInstance[=" + url + "]'s max_allowed_packet is " + variables.getMaxPacketSize() + ", but dble's is " + SystemConfig.getInstance().getMaxPacketSize();
                }
                LOGGER.warn(errMsg + ", set heartbeat Error");
                if (variables != null) {
                    AlertUtil.alert(AlarmCode.DB_INSTANCE_LOWER_CASE_ERROR, Alert.AlertLevel.WARN, errMsg, "mysql", this.heartbeat.getSource().getConfig().getId(), labels);
                    ToResolveContainer.DB_INSTANCE_LOWER_CASE_ERROR.add(url);
                }
                heartbeat.setErrorResult(errMsg);
                return true;
            } else {
                String url = heartbeat.getSource().getConfig().getUrl();
                if (ToResolveContainer.DB_INSTANCE_LOWER_CASE_ERROR.contains(url)) {
                    Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", url);
                    AlertUtil.alertResolve(AlarmCode.DB_INSTANCE_LOWER_CASE_ERROR, Alert.AlertLevel.WARN, "mysql", this.heartbeat.getSource().getConfig().getId(), labels,
                            ToResolveContainer.DB_INSTANCE_LOWER_CASE_ERROR, url);
                }
                if (!source.isSalveOrRead()) { // writehost checkRecoverFail read only
                    source.setReadOnly(variables.isReadOnly());
                }
            }
        }
        return false;
    }

    private void setStatusBySlave(PhysicalDbInstance source, Map<String, String> resultResult) {
        String slaveIoRunning = resultResult != null ? resultResult.get("Slave_IO_Running") : null;
        String slaveSqlRunning = resultResult != null ? resultResult.get("Slave_SQL_Running") : null;
        if (slaveIoRunning != null && slaveIoRunning.equals(slaveSqlRunning) && slaveSqlRunning.equals("Yes")) {
            heartbeat.setDbSynStatus(MySQLHeartbeat.DB_SYN_NORMAL);
            String secondsBehindMaster = resultResult.get("Seconds_Behind_Master");
            if (null != secondsBehindMaster && !"".equals(secondsBehindMaster) && !"NULL".equalsIgnoreCase(secondsBehindMaster)) {
                int behindMaster = Integer.parseInt(secondsBehindMaster);
                int delayThreshold = source.getDbGroupConfig().getDelayThreshold();
                if (delayThreshold > 0 && behindMaster > delayThreshold) {
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
        if (checkRecoverFail(source)) return;
        heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
    }

    private void setStatusByReadOnly(PhysicalDbInstance source, Map<String, String> resultResult) {
        String readonly = resultResult != null ? resultResult.get("@@read_only") : null;
        if (readonly == null) {
            heartbeat.setErrorResult("result of select @@read_only is null");
            return;
        } else if (readonly.equals("0")) {
            source.setReadOnly(false);
        } else {
            source.setReadOnly(true);
        }
        if (checkRecoverFail(source)) return;
        heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
    }

    public void close() {
        HeartbeatSQLJob curJob = sqlJob;
        if (curJob != null) {
            curJob.terminate();
            sqlJob = null;
        }
    }
}
