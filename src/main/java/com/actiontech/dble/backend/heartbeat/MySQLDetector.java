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
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * @author mycat
 */
public abstract class MySQLDetector implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLDetector.class);

    private volatile long lastSendQryTime;
    private volatile long lastReceivedQryTime;

    protected final MySQLHeartbeat heartbeat;
    protected HeartbeatSQLJob sqlJob;

    public MySQLDetector(MySQLHeartbeat heartbeat) {
        this.heartbeat = heartbeat;
    }

    public void heartbeat() {
        if (lastSendQryTime <= 0) {
            lastSendQryTime = System.currentTimeMillis();
            heartbeat.getSource().createConnectionSkipPool(null, sqlJob);
        } else {
            lastSendQryTime = System.currentTimeMillis();
            sqlJob.execute();
        }
    }

    boolean isHeartbeatTimeout() {
        return System.currentTimeMillis() > Math.max(lastSendQryTime, lastReceivedQryTime) + heartbeat.getHeartbeatTimeout();
    }

    long getLastReceivedQryTime() {
        return lastReceivedQryTime;
    }

    public void quit() {
        sqlJob.terminate();
    }

    public boolean isQuit() {
        return sqlJob.isQuit();
    }

    @Override
    public void onResult(SQLQueryResult<Map<String, String>> result) {
        lastReceivedQryTime = System.currentTimeMillis();
        heartbeat.getRecorder().set((lastReceivedQryTime - lastSendQryTime));
        if (result.isSuccess()) {
            PhysicalDbInstance source = heartbeat.getSource();
            Map<String, String> resultResult = result.getResult();
            setStatus(source, resultResult);
            if (checkRecoverFail(source)) return;
            heartbeat.setResult(MySQLHeartbeat.OK_STATUS);
        }
    }

    protected abstract void setStatus(PhysicalDbInstance source, Map<String, String> resultResult);

    public long getHeartbeatConnId() {
        if (sqlJob != null) {
            return sqlJob.getConnectionId();
        } else {
            return 0L;
        }
    }

    /**
     * if recover failed, return true
     */
    private boolean checkRecoverFail(PhysicalDbInstance source) {
        if (heartbeat.isStop()) {
            LOGGER.warn("heartbeat[{}] had been stop", source.getConfig().getUrl());
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
                if (heartbeat.getStatus() == MySQLHeartbeat.INIT_STATUS || !source.isSalveOrRead()) { // writehost checkRecoverFail read only
                    source.setReadOnly(variables.isReadOnly());
                }
            }
        }
        return false;
    }
}
