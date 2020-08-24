/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.statistic.DbInstanceSyncRecorder;
import com.actiontech.dble.statistic.HeartbeatRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author mycat
 */
public class MySQLHeartbeat {

    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLHeartbeat.class);
    public static final int DB_SYN_ERROR = -1;
    public static final int DB_SYN_NORMAL = 1;
    public static final String CHECK_STATUS_CHECKING = "checking";
    public static final String CHECK_STATUS_IDLE = "idle";

    public static final int INIT_STATUS = 0;
    public static final int OK_STATUS = 1;
    private static final int ERROR_STATUS = -1;
    static final int TIMEOUT_STATUS = -2;
    private final int errorRetryCount;
    private final AtomicBoolean isChecking = new AtomicBoolean(false);
    private final HeartbeatRecorder recorder = new HeartbeatRecorder();
    private final DbInstanceSyncRecorder asyncRecorder = new DbInstanceSyncRecorder();
    private final PhysicalDbInstance source;
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
    private volatile ScheduledFuture scheduledFuture;
    private AtomicLong errorTimeInLast5Min = new AtomicLong();
    private int errorTimeInLast5MinCount = 0;

    public MySQLHeartbeat(PhysicalDbInstance dbInstance) {
        this.source = dbInstance;
        this.status = INIT_STATUS;
        this.errorRetryCount = dbInstance.getDbGroupConfig().getErrorRetryCount();
        this.heartbeatTimeout = dbInstance.getDbGroupConfig().getHeartbeatTimeout();
        this.heartbeatSQL = dbInstance.getDbGroupConfig().getHeartbeatSQL();
    }

    public String getMessage() {
        return message;
    }

    public PhysicalDbInstance getSource() {
        return source;
    }

    public void setScheduledFuture(ScheduledFuture scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
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

    public void stop(String reason) {
        if (isStop) {
            return;
        }
        LOGGER.info("stop heartbeat of instance[{}], due to {}", source.getName(), reason);
        isStop = true;
        scheduledFuture.cancel(false);
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
            if (detector == null || detector.isQuit()) {
                detector = new MySQLDetector(this);
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
        //reset errorTimeInLast5Min
        if (null != errorTimeInLast5Min && errorTimeInLast5Min.intValue() != 0 && System.currentTimeMillis() >= errorTimeInLast5Min.get() + MINUTES.toMillis(5)) {
            errorTimeInLast5Min.set(System.currentTimeMillis());
            errorTimeInLast5MinCount = 0;
        }
    }

    // only use when heartbeat connection is closed
    boolean doHeartbeatRetry() {
        if (errorRetryCount > 0 && ++errorCount <= errorRetryCount) {
            // should continue checking error status
            if (detector != null) {
                detector.quit();
            }
            isChecking.set(false);
            LOGGER.warn("retry to do heartbeat for the " + errorCount + " times");
            heartbeat(); // error count not enough, heart beat again
            recordErrorCount();
            return true;
        }
        return false;
    }

    void setErrorResult(String errMsg) {
        LOGGER.warn("heartbeat to [" + source.getConfig().getUrl() + "] setError");
        // should continue checking error status
        if (detector != null) {
            detector.quit();
        }
        this.isChecking.set(false);
        this.message = errMsg;
        this.status = ERROR_STATUS;
        startErrorTime.compareAndSet(-1, System.currentTimeMillis());
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
        AlertUtil.alert(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "heartbeat status:" + this.status, "mysql", this.source.getConfig().getId(), labels);
        if (errorRetryCount > 0 && ++errorCount <= errorRetryCount) {
            LOGGER.warn("retry to do heartbeat for the " + errorCount + " times");
            heartbeat(); // error count not enough, heart beat again
            recordErrorCount();
        }
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
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
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
                Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
                AlertUtil.alertResolve(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "mysql", this.source.getConfig().getId(), labels);
        }
        if (isStop) {
            detector.quit();
        }
    }

    public void recordErrorCount() {
        long currentTimeMillis = System.currentTimeMillis();
        if (errorTimeInLast5Min.intValue() == 0) {
            errorTimeInLast5Min.set(currentTimeMillis);
        }
        if (errorTimeInLast5MinCount < errorRetryCount) {
            errorTimeInLast5MinCount++;
        }
    }


    private void setTimeout() {
        LOGGER.warn("heartbeat to [" + source.getConfig().getUrl() + "] setTimeout");
        status = TIMEOUT_STATUS;
    }

    public boolean isHeartBeatOK() {
        if (status == OK_STATUS || status == INIT_STATUS) {
            return true;
        } else if (status == ERROR_STATUS) {
            long timeDiff = System.currentTimeMillis() - this.startErrorTime.longValue();
            if (timeDiff >= heartbeatTimeout) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("error heartbeat continued for more than " + timeDiff + " Milliseconds and heartbeat Timeout is " + heartbeatTimeout + " Milliseconds");
                }
                return false;
            }
            return true;
        } else { // TIMEOUT_STATUS
            return false;
        }
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

    String getHeartbeatSQL() {
        return heartbeatSQL;
    }

    public DbInstanceSyncRecorder getAsyncRecorder() {
        return this.asyncRecorder;
    }

    public int getErrorTimeInLast5MinCount() {
        return errorTimeInLast5MinCount;
    }
}
