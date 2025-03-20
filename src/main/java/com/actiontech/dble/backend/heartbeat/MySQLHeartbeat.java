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
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.Scheduler;
import com.actiontech.dble.statistic.DbInstanceSyncRecorder;
import com.actiontech.dble.statistic.HeartbeatRecorder;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public static final int INIT_STATUS = 0;
    public static final int OK_STATUS = 1;
    public static final int ERROR_STATUS = -1;
    public static final int TIMEOUT_STATUS = -2;

    private final AtomicBoolean isChecking = new AtomicBoolean(false);
    private final PhysicalDbInstance source;
    protected volatile int status;
    private String heartbeatSQL;

    private final int errorRetryCount;  // when heartbeat error, dble retry count
    private final long heartbeatTimeout; // during the time, heart failed will ignore
    private volatile boolean isStop = true;
    private MySQLDetector detector;
    private volatile String message;
    private ScheduledFuture scheduledFuture;

    private final AtomicInteger errorCount = new AtomicInteger(0);
    private AtomicLong startErrorTime = new AtomicLong(-1L);
    private AtomicLong errorTimeInLast5Min = new AtomicLong();
    private int errorTimeInLast5MinCount = 0;

    private final HeartbeatRecorder recorder = new HeartbeatRecorder();

    private boolean isDelayDetection;
    private volatile int logicUpdate = 0;

    private volatile int dbSynStatus = DB_SYN_NORMAL;
    private volatile Integer slaveBehindMaster;
    private final DbInstanceSyncRecorder asyncRecorder = new DbInstanceSyncRecorder();

    public MySQLHeartbeat(PhysicalDbInstance dbInstance) {
        this.source = dbInstance;
        this.status = INIT_STATUS;
        this.errorRetryCount = dbInstance.getDbGroupConfig().getErrorRetryCount();
        this.heartbeatTimeout = dbInstance.getDbGroupConfig().getHeartbeatTimeout();
        this.isDelayDetection = dbInstance.getDbGroupConfig().isDelayDetection();
        if (isDelayDetection) {
            this.heartbeatSQL = getDetectorSql(dbInstance.getDbGroupConfig().getName(), dbInstance.getDbGroupConfig().getDelayDatabase());
        } else {
            this.heartbeatSQL = source.getDbGroupConfig().getHeartbeatSQL();
        }
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

    public void start(long heartbeatRecoveryTime) {
        LOGGER.info("start heartbeat of instance[{}]", source);
        if (Objects.nonNull(scheduledFuture)) {
            stop("the legacy thread is not closed");
        }
        isStop = false;

        long heartbeatPeriodMillis;
        long initialDelay = 0;
        if (isDelayDetection) {
            heartbeatPeriodMillis = source.getDbGroupConfig().getDelayPeriodMillis();
            if (source.isReadInstance()) {
                initialDelay = source.getDbGroupConfig().getDelayPeriodMillis();
            }
        } else {
            heartbeatPeriodMillis = (int) source.getConfig().getPoolConfig().getHeartbeatPeriodMillis();
        }

        this.scheduledFuture = Scheduler.getInstance().getScheduledExecutor().scheduleAtFixedRate(() -> {
            if (DbleServer.getInstance().getConfig().isFullyConfigured()) {
                if (TimeUtil.currentTimeMillis() < heartbeatRecoveryTime) {
                    return;
                }

                heartbeat();
            }
        }, initialDelay, heartbeatPeriodMillis, TimeUnit.MILLISECONDS);
    }

    public void stop(String reason) {
        if (isStop) {
            LOGGER.warn("heartbeat[{}] had been stop", source.getConfig().getUrl());
            return;
        }
        LOGGER.info("stop heartbeat of instance[{}], due to {}", source.getConfig().getUrl(), reason);
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
                detector = getMySQLDetector();
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

    private String getDetectorSql(String dbGroupName, String delayDatabase) {
        String[] str = {"dble", dbGroupName, SystemConfig.getInstance().getInstanceName()};
        String sourceName = Joiner.on("_").join(str);
        String sqlTableName = delayDatabase + ".u_delay ";
        String detectorSql;
        if (!source.isReadInstance()) {
            String update = "replace into ? (source,real_timestamp,logic_timestamp) values ('?','?',?)";
            detectorSql = convert(update, Lists.newArrayList(sqlTableName, sourceName));
        } else {
            String select = "select logic_timestamp from ? where source = '?'";
            detectorSql = convert(select, Lists.newArrayList(sqlTableName, sourceName));
        }
        return detectorSql;
    }

    private String convert(String template, List<String> list) {
        StringBuilder sb = new StringBuilder(template);
        String replace = "?";
        for (String str : list) {
            int index = sb.indexOf(replace);
            sb.replace(index, index + 1, str);
        }
        return sb.toString();
    }

    private MySQLDetector getMySQLDetector() {
        if (isDelayDetection) {
            return new MySQLDelayDetector(this);
        } else if (source.getDbGroupConfig().isShowSlaveSql()) {
            return new MySQLShowSlaveStatusDetector(this);
        } else if (source.getDbGroupConfig().isSelectReadOnlySql()) {
            return new MySQLReadOnlyDetector(this);
        } else {
            return new MySQLDefaultDetector(this);
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
        this.status = ERROR_STATUS;
        startErrorTime.compareAndSet(-1, System.currentTimeMillis());
        alert();
        if (errorRetryCount > 0 && errorCount.get() < errorRetryCount) {
            LOGGER.warn("retry to do heartbeat for the " + errorCount.incrementAndGet() + " times");
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
            alert();
        }
    }

    private void alert() {
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", this.source.getDbGroupConfig().getName() + "-" + this.source.getConfig().getInstanceName());
        AlertUtil.alert(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "heartbeat status:" + this.status, "mysql", this.source.getConfig().getId(), labels);
    }

    private void setOk() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("heartbeat to [" + source.getConfig().getUrl() + "] setOK");
        }
        switch (status) {
            case TIMEOUT_STATUS:
                this.status = INIT_STATUS;
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
            case OK_STATUS:
                break;
            default:
                LOGGER.info("heartbeat to [{}] setOk, previous status is {}", source.getConfig().getUrl(), status);
                this.status = OK_STATUS;
                this.errorCount.set(0);
                this.startErrorTime.set(-1);
                alert();
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
        if (status != TIMEOUT_STATUS) {
            LOGGER.warn("heartbeat to [{}] setTimeout, previous status is {}", source.getConfig().getUrl(), status);
            status = TIMEOUT_STATUS;
        }
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
        return errorCount.get();
    }

    public HeartbeatRecorder getRecorder() {
        return recorder;
    }

    public long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    String getHeartbeatSQL() {
        if (isDelayDetection && !source.isReadInstance()) {
            return convert(heartbeatSQL, Lists.newArrayList(String.valueOf(LocalDateTime.now()), String.valueOf(source.getDbGroup().getLogicTimestamp().incrementAndGet())));
        } else {
            return heartbeatSQL;
        }
    }

    public DbInstanceSyncRecorder getAsyncRecorder() {
        return this.asyncRecorder;
    }

    public int getErrorTimeInLast5MinCount() {
        return errorTimeInLast5MinCount;
    }

    public long getHeartbeatConnId() {
        if (detector != null) {
            return detector.getHeartbeatConnId();
        } else {
            return 0L;
        }
    }

    public int getLogicUpdate() {
        return logicUpdate;
    }

    public void setLogicUpdate(int logicUpdate) {
        this.logicUpdate = logicUpdate;
    }
}
