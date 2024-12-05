/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.delyDetection;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.db.DbGroupConfig;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.singleton.Scheduler;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DelayDetection {
    public static final Logger LOGGER = LoggerFactory.getLogger(DelayDetection.class);

    private boolean tableExists;
    private volatile boolean stop = true;
    private volatile BackendConnection conn;
    private volatile ScheduledFuture scheduledFuture;
    private final PhysicalDbInstance source;
    private final DbGroupConfig dbGroupConfig;
    private DelayDetectionStatus delayDetectionStatus;
    private DelayDetectionTask delayDetectionTask;
    private int delayThreshold;
    private int delayPeriodMillis;
    private AtomicLong version = new AtomicLong();
    private AtomicBoolean isChecking = new AtomicBoolean();
    private volatile LocalDateTime lastSendQryTime = LocalDateTime.now();
    private volatile LocalDateTime lastReceivedQryTime = LocalDateTime.now();
    private volatile long delayVal = 0;
    private volatile int logicUpdate = 0;
    private AtomicInteger errorCount = new AtomicInteger();
    private int errorRetryCount = 3;

    //table source field
    private String sourceName;
    private String sqlTableName;
    private String updateSQL;
    private String selectSQL;
    private String createTableSQL;
    private String errorMessage;


    public DelayDetection(PhysicalDbInstance source) {
        this.source = source;
        this.dbGroupConfig = source.getDbGroupConfig();
        delayThreshold = dbGroupConfig.getDelayThreshold();
        delayPeriodMillis = dbGroupConfig.getDelayPeriodMillis();
        delayDetectionStatus = DelayDetectionStatus.INIT;
        source.setDelayDetectionStatus(delayDetectionStatus);
        delayDetectionTask = new DelayDetectionTask(this);
        synSql();
    }

    private void synSql() {
        String[] str = {"OBsharding-D", dbGroupConfig.getName(), SystemConfig.getInstance().getInstanceName()};
        sourceName = Joiner.on("_").join(str);
        String schema = dbGroupConfig.getDelayDatabase();
        String tableName = ".u_delay ";
        sqlTableName = schema + tableName;
        StringBuilder select = new StringBuilder("select logic_timestamp from ? where source = '?'");
        selectSQL = convert(select, Lists.newArrayList(sqlTableName, sourceName));
        StringBuilder create = new StringBuilder("create table if not exists ? (source VARCHAR(256) primary key,real_timestamp varchar(26) NOT NULL,logic_timestamp BIGINT default 0)");
        createTableSQL = convert(create, Lists.newArrayList(sqlTableName));
    }

    private String convert(StringBuilder template, List<String> list) {
        StringBuilder sb = new StringBuilder(template);
        String replace = "?";
        for (String str : list) {
            int index = sb.indexOf(replace);
            sb.replace(index, index + 1, str);
        }
        return sb.toString();
    }

    public void start(long initialDelay) {
        LOGGER.info("start delayDetection of instance[{}]", source);
        if (Objects.nonNull(scheduledFuture)) {
            stop("the legacy thread is not closed");
        }
        stop = false;
        if (initialDelay > 0) {
            //avoid concurrency with the master
            initialDelay = initialDelay >> 1;
        }
        this.scheduledFuture = Scheduler.getInstance().getScheduledExecutor().scheduleAtFixedRate(() -> execute(),
                initialDelay, this.delayPeriodMillis, TimeUnit.MILLISECONDS);
    }

    public void execute() {
        if (isChecking.compareAndSet(false, true)) {
            if (delayDetectionTask.isQuit()) {
                delayDetectionTask = new DelayDetectionTask(this);
            }
            if (!source.isReadInstance()) {
                StringBuilder update = new StringBuilder("replace into ? (source,real_timestamp,logic_timestamp) values ('?','?',?)");
                List<String> strings = Lists.newArrayList(sqlTableName, sourceName, String.valueOf(LocalDateTime.now()), String.valueOf(source.getDbGroup().getLogicTimestamp().incrementAndGet()));
                updateSQL = convert(update, strings);
            }
            delayDetectionTask.execute();
        } else {
            if (errorCount.get() > 0) {
                LOGGER.warn("may retry, no need for this scheduled check");
                return;
            }
            LocalDateTime result = lastReceivedQryTime;
            if (lastSendQryTime.getNano() > lastReceivedQryTime.getNano()) {
                result = lastSendQryTime;
            }
            Duration duration = Duration.between(result, LocalDateTime.now());
            if (duration.toMillis() > delayThreshold) {
                if (source.isReadInstance()) {
                    delayVal = -1;
                }
                errorMessage = "connection did not respond after the delayThreshold time was exceeded";
                setResult(DelayDetectionStatus.TIMEOUT);
            }
        }
    }

    public void stop(String reason) {
        LOGGER.info("stop delayDetection of instance[{}], due to {}", source, reason);
        stop = true;
        if (Objects.nonNull(scheduledFuture)) {
            scheduledFuture.cancel(true);
            delayDetectionStatus = DelayDetectionStatus.STOP;
            source.setDelayDetectionStatus(delayDetectionStatus);
            cancel(reason);
            scheduledFuture = null;
        }
    }

    public void setResult(DelayDetectionStatus result) {
        isChecking.set(false);
        switch (result) {
            case OK:
                setOk();
                errorMessage = null;
                break;
            case TIMEOUT:
                setTimeout();
                break;
            case ERROR:
                setError();
                break;
            default:
                break;
        }

    }

    public void cancel(String reason) {
        LOGGER.warn("delayDetection need cancel ,reason is {}", reason);
        delayDetectionTask.close();
        updateLastReceivedQryTime();
        version.set(0);
        final BackendConnection connection = conn;
        if (Objects.nonNull(connection) && !connection.isClosed()) {
            connection.businessClose(reason);
        }
        conn = null;
        errorMessage = reason;
        setResult(DelayDetectionStatus.ERROR);
    }

    public void delayCal(long delay) {
        PhysicalDbGroup dbGroup = source.getDbGroup();
        long logic = dbGroup.getLogicTimestamp().get();
        long result = logic - delay;
        // master-slave switch need ignore
        if (result < 0) {
            result = 0;
        }
        DelayDetectionStatus writeDbStatus = dbGroup.getWriteDbInstance().getDelayDetectionStatus();
        delayVal = result * delayPeriodMillis;

        //writeDbStatus is error,salve was considered normal
        if (writeDbStatus == DelayDetectionStatus.ERROR || writeDbStatus == DelayDetectionStatus.TIMEOUT) {
            setResult(DelayDetectionStatus.OK);
            return;
        }
        if (delayThreshold > delayVal) {
            setResult(DelayDetectionStatus.OK);
        } else {
            errorMessage = "found MySQL master/slave Replication delay, instance is  " + source.getConfig() + " ,sync delay time: " + delayVal + " ms";
            setResult(DelayDetectionStatus.TIMEOUT);
        }
    }

    public String getLastReceivedQryTime() {
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return pattern.format(lastReceivedQryTime);
    }

    public void delayDetectionRetry() {
        if (errorCount.incrementAndGet() <= errorRetryCount) {
            execute();
        }
    }

    public void updateLastSendQryTime() {
        lastSendQryTime = LocalDateTime.now();
    }

    public void updateLastReceivedQryTime() {
        lastReceivedQryTime = LocalDateTime.now();
    }

    private void setTimeout() {
        LOGGER.warn("delayDetection to [" + source.getConfig().getUrl() + "] setTimeout");
        delayDetectionStatus = DelayDetectionStatus.TIMEOUT;
        source.setDelayDetectionStatus(delayDetectionStatus);
        alert(AlarmCode.DB_SLAVE_INSTANCE_DELAY, errorMessage, dbGroupConfig.instanceDatabaseType().name().toLowerCase());
    }

    private void setError() {
        LOGGER.warn("delayDetection to [" + source.getConfig().getUrl() + "] setError");
        delayDetectionStatus = DelayDetectionStatus.ERROR;
        source.setDelayDetectionStatus(delayDetectionStatus);
        if (!source.isReadInstance()) {
            alert(AlarmCode.DB_MASTER_INSTANCE_DELAY_FAIL, "reason is " + errorMessage + " delayDetection status: " + delayDetectionStatus, dbGroupConfig.instanceDatabaseType().name().toLowerCase());
        }
    }

    private void setOk() {
        LOGGER.debug("delayDetection to [" + source.getConfig().getUrl() + "] setOK");
        if (errorCount.get() > 0) {
            errorCount.set(0);
        }
        if (!source.isReadInstance()) {
            delayVal = 0;
        }
        delayDetectionStatus = DelayDetectionStatus.OK;
        source.setDelayDetectionStatus(delayDetectionStatus);
    }

    private void alert(String alarmCode, String errMsg, String databaseType) {
        String alertKey = source.getDbGroupConfig().getName() + "-" + source.getConfig().getInstanceName();
        Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", alertKey);
        AlertUtil.alert(alarmCode, Alert.AlertLevel.WARN, errMsg, databaseType, source.getConfig().getId(), labels);
    }

    public PhysicalDbInstance getSource() {
        return source;
    }

    public BackendConnection getConn() {
        return conn;
    }

    public void setConn(BackendConnection conn) {
        this.conn = conn;
    }

    public AtomicLong getVersion() {
        return version;
    }

    public void setVersion(AtomicLong version) {
        this.version = version;
    }

    public String getUpdateSQL() {
        return updateSQL;
    }

    public String getSelectSQL() {
        return selectSQL;
    }

    public boolean isStop() {
        return stop;
    }

    public boolean isTableExists() {
        return tableExists;
    }

    public void setTableExists(boolean tableExists) {
        this.tableExists = tableExists;
    }

    public String getCreateTableSQL() {
        return createTableSQL;
    }

    public long getDelayVal() {
        return delayVal;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getLogicUpdate() {
        return logicUpdate;
    }

    public void setLogicUpdate(int logicUpdate) {
        this.logicUpdate = logicUpdate;
    }
}



