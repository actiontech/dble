package com.actiontech.dble.backend.delyDetection;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.singleton.Scheduler;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private StringBuilder updatePrefix;
    //table source field
    private String name;
    private String updateSQL;
    private String selectSQL;
    private String crateTabletSQL;
    private String errorMessage;


    public DelayDetection(PhysicalDbInstance source) {
        this.source = source;
        this.dbGroupConfig = source.getDbGroupConfig();
        delayThreshold = dbGroupConfig.getDelayThreshold();
        delayPeriodMillis = dbGroupConfig.getDelayPeriodMillis();
        delayDetectionStatus = DelayDetectionStatus.INIT;
        delayDetectionTask = new DelayDetectionTask(this);
        synSql();
    }

    private void synSql() {
        String[] str = {"dble", SystemConfig.getInstance().getInstanceName(), dbGroupConfig.getName(), source.getName()};
        name = Joiner.on("_").join(str);
        String schema = dbGroupConfig.getDelayDatabase();
        String tableName = ".u_delay ";
        updatePrefix = new StringBuilder("replace into ");
        updatePrefix.append(schema).append(tableName);

        StringBuilder select = new StringBuilder("select logic_timestamp from ");
        select.append(schema).append(tableName)
                .append("where source = ")
                .append("'").append(name).append("'");
        selectSQL = select.toString();

        StringBuilder create = new StringBuilder("create table if not exists ");
        create.append(schema)
                .append(tableName)
                .append("(")
                .append("source VARCHAR(256) primary key,")
                .append("real_timestamp varchar(26) NOT NULL,")
                .append("logic_timestamp BIGINT default 0")
                .append(")");
        crateTabletSQL = create.toString();
    }

    public void start(long initialDelay) {
        LOGGER.info("start delayDetection of instance[{}]", source);
        if (Objects.nonNull(scheduledFuture)) {
            stop("the legacy thread is not closed");
        }
        stop = false;
        this.scheduledFuture = Scheduler.getInstance().getScheduledExecutor().scheduleAtFixedRate(() -> execute(),
                initialDelay, delayPeriodMillis, TimeUnit.MILLISECONDS);
    }

    public void execute() {
        if (isChecking.compareAndSet(false, true)) {
            if (delayDetectionTask.isQuit()) {
                delayDetectionTask = new DelayDetectionTask(this);
            }
            if (!source.isReadInstance()) {
                String quotes = "'";
                String comma = ",";
                StringBuilder update = new StringBuilder(updatePrefix);
                update.append("(").append("source,").append("real_timestamp,").append("logic_timestamp").append(")").append(" values ")
                        .append("(")
                        .append(quotes).append(name).append(quotes).append(comma)
                        .append(quotes).append(LocalDateTime.now()).append(quotes).append(comma)
                        .append(source.getDbGroup().getLogicTimestamp().incrementAndGet())
                        .append(")");
                updateSQL = update.toString();
            }
            delayDetectionTask.execute();
        } else {
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
        errorMessage = null;
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
            errorMessage = "found MySQL master/slave Replication delay" + source.getConfig() + ",sync time delay: " + delayVal + " ms";
            setResult(DelayDetectionStatus.TIMEOUT);
        }
    }

    public LocalDateTime getLastReceivedQryTime() {
        return lastReceivedQryTime;
    }

    public void delayDetectionRetry() {
        execute();
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
        alert(AlarmCode.DB_SLAVE_INSTANCE_DELAY, errorMessage, dbGroupConfig.instanceDatabaseType().name());
    }

    private void setError() {
        LOGGER.warn("delayDetection to [" + source.getConfig().getUrl() + "] setError");
        delayDetectionStatus = DelayDetectionStatus.ERROR;
        source.setDelayDetectionStatus(delayDetectionStatus);
        if (!source.isReadInstance()) {
            alert(AlarmCode.DB_MASTER_INSTANCE_DELAY_FAIL, "reason is " + errorMessage + "delayDetection status:" + delayDetectionStatus, dbGroupConfig.instanceDatabaseType().name());
        }
    }

    private void setOk() {
        LOGGER.debug("delayDetection to [" + source.getConfig().getUrl() + "] setOK");
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

    public String getCrateTabletSQL() {
        return crateTabletSQL;
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



