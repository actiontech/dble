/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.statistic.DataSourceSyncRecorder;
import com.actiontech.dble.statistic.HeartbeatRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public class MySQLHeartbeat {

    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLHeartbeat.class);
    public static final int DB_SYN_ERROR = -1;
    public static final int DB_SYN_NORMAL = 1;

    public static final int INIT_STATUS = 0;
    public static final int OK_STATUS = 1;
    private static final int ERROR_STATUS = -1;
    static final int TIMEOUT_STATUS = -2;
    private final int errorRetryCount;
    private final AtomicBoolean isChecking = new AtomicBoolean(false);
    private final HeartbeatRecorder recorder = new HeartbeatRecorder();
    private final DataSourceSyncRecorder asyncRecorder = new DataSourceSyncRecorder();
    private final MySQLDataSource source;
    protected volatile int status;
    private String heartbeatSQL;
    private long heartbeatTimeout; // during the time, heart failed will ignore
    private volatile int errorCount = 0;
    private AtomicLong startErrorTime = new AtomicLong(-1L);
    private volatile boolean isStop = true;
    private volatile int dbSynStatus = DB_SYN_NORMAL;
    private volatile Integer slaveBehindMaster;
    private MySQLDetector detector;
    private volatile String message;

    public MySQLHeartbeat(MySQLDataSource source) {
        this.source = source;
        this.status = INIT_STATUS;
        this.errorRetryCount = source.getHostConfig().getErrorRetryCount();
        this.heartbeatTimeout = source.getHostConfig().getHeartbeatTimeout();
        this.heartbeatSQL = source.getHostConfig().getHearbeatSQL();
    }

    public String getMessage() {
        return message;
    }

    public MySQLDataSource getSource() {
        return source;
    }

    public String getLastActiveTime() {
        if (detector == null) {
            return null;
        }
        long t = detector.getLastReceivedQryTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(t));
    }

    public void start() {
        isStop = false;
    }

    public void stop() {
        isStop = true;
        this.status = INIT_STATUS;
        if (detector != null && !detector.isQuit()) {
            detector.quit();
            isChecking.set(false);
        }
    }

    /**
     * execute heart beat
     */
    public void heartbeat() {
        if (isChecking.compareAndSet(false, true)) {
            if (detector == null) {
                detector = new MySQLDetector(this);
            } else if (detector.isQuit()) {
                detector = new MySQLDetector(this, detector.getLastReceivedQryTime());
            }

            detector.heartbeat();
        } else {
            if (detector != null) {
                if (detector.isQuit()) {
                    isChecking.set(false);
                } else if (detector.isHeartbeatTimeout()) {
                    setResult(TIMEOUT_STATUS);
                }
            }
        }
        if (isStop) {
            stop();
        }
    }

    public void setErrorResult(String errMsg) {
        this.isChecking.set(false);
        this.message = errMsg;
        setError();
        Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.source.getHostConfig().getName() + "-" + this.source.getConfig().getHostName());
        AlertUtil.alert(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "heartbeat status:" + this.status, "mysql", this.source.getConfig().getId(), labels);
    }

    void setResult(int result) {
        this.isChecking.set(false);
        this.message = null;
        switch (result) {
            case OK_STATUS:
                setOk();
                break;
            case TIMEOUT_STATUS:
                setTimeout();
                break;
            default:
                break;
        }
        if (this.status != OK_STATUS) {
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.source.getHostConfig().getName() + "-" + this.source.getConfig().getHostName());
            AlertUtil.alert(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "heartbeat status:" + this.status, "mysql", this.source.getConfig().getId(), labels);
        }
    }

    private void setOk() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("heartbeat to [" + source.getConfig().getUrl() + "] setOK");
        }
        switch (status) {
            case TIMEOUT_STATUS:
                this.status = INIT_STATUS;
                this.errorCount = 0;
                this.startErrorTime.set(-1);
                if (isStop) {
                    detector.quit();
                } else {
                    heartbeat(); // timeout, heart beat again
                }
                break;
            case OK_STATUS:
                break;
            default:
                this.status = OK_STATUS;
                this.errorCount = 0;
                this.startErrorTime.set(-1);
                Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.source.getHostConfig().getName() + "-" + this.source.getConfig().getHostName());
                AlertUtil.alertResolve(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "mysql", this.source.getConfig().getId(), labels);
        }
        if (isStop) {
            detector.quit();
        }
    }

    private void setError() {
        LOGGER.warn("heartbeat to [" + source.getConfig().getUrl() + "] setError");
        // should continue checking error status
        if (detector != null) {
            detector.quit();
        }
        this.status = ERROR_STATUS;
        startErrorTime.compareAndSet(-1, System.currentTimeMillis());
        if (++errorCount <= errorRetryCount) {
            heartbeatRetry(); // error count not enough, heart beat again
        }
    }

    private void heartbeatRetry() {
        LOGGER.info("heartbeat failed, retry for the " + errorCount + " times");
        heartbeat();
    }

    private void setTimeout() {
        LOGGER.warn("heartbeat to [" + source.getConfig().getUrl() + "] setTimeout");
        this.isChecking.set(false);
        status = TIMEOUT_STATUS;
    }

    public Integer getSlaveBehindMaster() {
        return slaveBehindMaster;
    }

    void setSlaveBehindMaster(Integer slaveBehindMaster) {
        this.slaveBehindMaster = slaveBehindMaster;
    }

    public int getDbSynStatus() {
        return dbSynStatus;
    }

    void setDbSynStatus(int dbSynStatus) {
        this.dbSynStatus = dbSynStatus;
    }

    public int getStatus() {
        return status;
    }

    public boolean isChecking() {
        return isChecking.get();
    }

    public boolean isStop() {
        return isStop;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public HeartbeatRecorder getRecorder() {
        return recorder;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public boolean isHeartBeatOK() {
        if (status == OK_STATUS) {
            return true;
        } else if (status == INIT_STATUS) { // init or timeout->ok
            return false;
        } else if (status == ERROR_STATUS) {
            long timeDiff = System.currentTimeMillis() - this.startErrorTime.longValue();
            if (timeDiff >= heartbeatTimeout) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("error heartbaet continued for more than " + timeDiff + " Milliseconds and heartbeat Timeout is " + heartbeatTimeout + " Milliseconds");
                }
                return false;
            }
            return true;
        } else { // TIMEOUT_STATUS
            return false;
        }
    }

    public String getHeartbeatSQL() {
        return heartbeatSQL;
    }

    public DataSourceSyncRecorder getAsyncRecorder() {
        return this.asyncRecorder;
    }
}
