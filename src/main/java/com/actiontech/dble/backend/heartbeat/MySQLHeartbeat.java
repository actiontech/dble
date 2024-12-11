/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.singleton.Scheduler;
import com.actiontech.dble.statistic.DbInstanceSyncRecorder;
import com.actiontech.dble.statistic.HeartbeatRecorder;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final int errorRetryCount;
    private final AtomicBoolean isChecking = new AtomicBoolean(false);
    private final HeartbeatRecorder recorder = new HeartbeatRecorder();
    private final DbInstanceSyncRecorder asyncRecorder = new DbInstanceSyncRecorder();
    private final PhysicalDbInstance source;
    protected volatile MySQLHeartbeatStatus status;
    private volatile long beginTimeoutTime = 0;
    private String heartbeatSQL;
    private long heartbeatTimeout; // during the time, heart failed will ignore
    private final AtomicInteger errorCount = new AtomicInteger(0);
    private AtomicLong startErrorTime = new AtomicLong(-1L);
    private volatile boolean isStop = true;
    private volatile int dbSynStatus = DB_SYN_NORMAL;
    private volatile Integer slaveBehindMaster;
    private MySQLDetector detector;
    private volatile String message;
    private volatile ScheduledFuture scheduledFuture;
    private AtomicLong errorTimeInLast5Min = new AtomicLong();
    private int errorTimeInLast5MinCount = 0;
    private volatile long heartbeatRecoveryTime;
    private AtomicBoolean initHeartbeat = new AtomicBoolean(false);

    public MySQLHeartbeat(PhysicalDbInstance dbInstance) {
        this.source = dbInstance;
        this.status = MySQLHeartbeatStatus.INIT;
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

    public String getLastActiveTime() {
        if (detector == null) {
            return null;
        }
        long t = detector.getLastReceivedQryTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(t));
    }

    public void start(long heartbeatPeriodMillis) {
        this.isStop = false;
        if (initHeartbeat.compareAndSet(false, true)) {
            this.scheduledFuture = Scheduler.getInstance().getScheduledExecutor().scheduleAtFixedRate(() -> {
                if (DbleServer.getInstance().getConfig().isFullyConfigured()) {
                    if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
                        return;
                    }
                    heartbeat();
                }
            }, 0L, heartbeatPeriodMillis, TimeUnit.MILLISECONDS);
        } else {
            LOGGER.warn("init dbInstance[{}] heartbeat, but it has been initialized, skip initialization.", source.getName());
        }
    }

    public void stop(String reason) {
        if (isStop) {
            LOGGER.warn("heartbeat[{}] had been stop", source.getConfig().getUrl());
            return;
        }
        LOGGER.info("stop heartbeat of instance[{}], due to {}", source.getConfig().getUrl(), reason);
        isStop = true;
        scheduledFuture.cancel(false);
        initHeartbeat.set(false);
        this.status = MySQLHeartbeatStatus.STOP;
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
                    setResult(MySQLHeartbeatStatus.TIMEOUT);
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
        if (errorRetryCount > 0 && errorCount.get() < errorRetryCount) {
            // should continue checking error status
            if (detector != null) {
                detector.quit();
            }
            isChecking.set(false);
            LOGGER.warn("retry to do heartbeat for the " + errorCount.incrementAndGet() + " times");
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
        this.status = MySQLHeartbeatStatus.ERROR;
        startErrorTime.compareAndSet(-1, System.currentTimeMillis());
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
        AlertUtil.alert(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "heartbeat status:" + status + ", message: " + this.message, "mysql", this.source.getConfig().getId(), labels);
        if (errorRetryCount > 0 && errorCount.get() < errorRetryCount) {
            LOGGER.warn("retry to do heartbeat for the " + errorCount.incrementAndGet() + " times");
            heartbeat(); // error count not enough, heart beat again
            recordErrorCount();
        }
    }

    void setResult(MySQLHeartbeatStatus result) {
        this.isChecking.set(false);
        this.message = null;
        switch (result) {
            case OK:
                setOk();
                break;
            case TIMEOUT:
                setTimeout();
                break;
            default:
                break;
        }
        if (this.status != MySQLHeartbeatStatus.OK) {
            Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
            AlertUtil.alert(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "heartbeat status:" + status, "mysql", this.source.getConfig().getId(), labels);
        }
    }

    private void setOk() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("heartbeat to [" + source.getConfig().getUrl() + "] setOK");
        }
        MySQLHeartbeatStatus previousStatus = status;
        switch (status) {
            case TIMEOUT:
                this.status = MySQLHeartbeatStatus.INIT;
                this.errorCount.set(0);
                this.startErrorTime.set(-1);
                if (isStop) {
                    LOGGER.warn("heartbeat[{}] had been stop", source.getConfig().getUrl());
                    detector.quit();
                } else {
                    LOGGER.info("heartbeat to [{}] setOk, previous status is timeout", source.getConfig().getUrl());
                    heartbeat(); // timeout, heart beat again
                }
                break;
            case OK:
                break;
            default:
                LOGGER.info("heartbeat to [{}] setOk, previous status is {}", source.getConfig().getUrl(), status);
                this.status = MySQLHeartbeatStatus.OK;
                this.errorCount.set(0);
                this.startErrorTime.set(-1);
                Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
                AlertUtil.alertResolve(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "mysql", this.source.getConfig().getId(), labels);
        }
        //after the heartbeat changes from failure to success, it needs to be expanded immediately
        if (source.getTotalConnections() == 0 && !previousStatus.equals(MySQLHeartbeatStatus.INIT) && !previousStatus.equals(MySQLHeartbeatStatus.OK)) {
            LOGGER.debug("[updatePoolCapacity] heartbeat to [{}] setOk, previous status is {}", source, previousStatus);
            source.updatePoolCapacity();
        }
        if (isStop) {
            LOGGER.warn("heartbeat[{}] had been stop", source.getConfig().getUrl());
            detector.quit();
        }
    }

    private void recordErrorCount() {
        long currentTimeMillis = System.currentTimeMillis();
        if (errorTimeInLast5Min.intValue() == 0) {
            errorTimeInLast5Min.set(currentTimeMillis);
        }
        if (errorTimeInLast5MinCount < errorRetryCount) {
            errorTimeInLast5MinCount++;
        }
    }

    private void setTimeout() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("heartbeat to [" + source.getConfig().getUrl() + "] setTimeout");
        }
        if (status != MySQLHeartbeatStatus.TIMEOUT) {
            LOGGER.warn("heartbeat to [{}] setTimeout, previous status is {}", source.getConfig().getUrl(), status);
            beginTimeoutTime = System.currentTimeMillis();
            status = MySQLHeartbeatStatus.TIMEOUT;
        }
    }


    public long getBeginTimeoutTime() {
        return beginTimeoutTime;
    }

    public boolean isHeartBeatOK() {
        if (status == MySQLHeartbeatStatus.OK || status == MySQLHeartbeatStatus.INIT) {
            return true;
        } else if (status == MySQLHeartbeatStatus.ERROR) {
            long timeDiff = System.currentTimeMillis() - this.startErrorTime.longValue();
            if (timeDiff >= heartbeatTimeout) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("error heartbeat continued for more than " + timeDiff + " Milliseconds and heartbeat Timeout is " + heartbeatTimeout + " Milliseconds");
                }
                return false;
            }
            return true;
        } else { // TIMEOUT or STOP
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

    public MySQLHeartbeatStatus getStatus() {
        return status;
    }

    public boolean isChecking() {
        return isChecking.get();
    }

    public boolean isStop() {
        return isStop;
    }

    public int getErrorCount() {
        return errorCount.get();
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

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

}
