/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.config.model.DataHostConfig;
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
    static final int ERROR_STATUS = -1;
    static final int TIMEOUT_STATUS = -2;

    private String heartbeatSQL;
    private final int errorRetryCount;
    private long heartbeatTimeout; // during the time, heart failed will ignore
    private volatile int errorCount = 0;
    private AtomicLong startErrorTime = new AtomicLong(-1L);

    private volatile boolean isStop = true;
    private final AtomicBoolean isChecking = new AtomicBoolean(false);
    protected volatile int status;

    private final HeartbeatRecorder recorder = new HeartbeatRecorder();
    private final DataSourceSyncRecorder asyncRecorder = new DataSourceSyncRecorder();
    private volatile int dbSynStatus = DB_SYN_NORMAL;

    private volatile Integer slaveBehindMaster;
    private final MySQLDataSource source;
    private MySQLDetector detector;
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    public MySQLHeartbeat(MySQLDataSource source) {
        this.source = source;
        this.status = INIT_STATUS;
        this.errorRetryCount = source.getHostConfig().getErrorRetryCount();
        this.heartbeatTimeout = source.getHostConfig().getHeartbeatTimeout();
        this.heartbeatSQL = source.getHostConfig().getHearbeatSQL();
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
    }

    /**
     * execute heart beat
     */
    public void heartbeat() {
        if (isStop) {
            this.status = INIT_STATUS;
            if (detector != null && !detector.isQuit()) {
                detector.quit();
                isChecking.set(false);
            }
            return;
        }
        if (isChecking.compareAndSet(false, true)) {
            if (detector == null || detector.isQuit()) {
                try {
                    detector = new MySQLDetector(this);
                    detector.heartbeat();
                } catch (Exception e) {
                    LOGGER.info(source.getConfig().toString(), e);
                    setResult(ERROR_STATUS);
                }
            } else {
                detector.heartbeat();
            }
        } else {
            if (detector != null) {
                if (detector.isQuit()) {
                    isChecking.set(false);
                } else if (detector.isHeartbeatTimeout()) {
                    setResult(TIMEOUT_STATUS);
                }
            }
        }
    }

    void setResult(int result) {
        this.isChecking.set(false);
        switch (result) {
            case OK_STATUS:
                setOk();
                break;
            case ERROR_STATUS:
                setError();
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
            switchSourceIfNeed("heartbeat error");
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
        // should continues check error status
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

    public int getDbSynStatus() {
        return dbSynStatus;
    }

    void setDbSynStatus(int dbSynStatus) {
        this.dbSynStatus = dbSynStatus;
    }

    void setSlaveBehindMaster(Integer slaveBehindMaster) {
        this.slaveBehindMaster = slaveBehindMaster;
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


    /**
     * switch data source
     */
    private void switchSourceIfNeed(String reason) {
        int switchType = source.getHostConfig().getSwitchType();
        if (switchType == DataHostConfig.NOT_SWITCH_DS) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("not switch datasource, for switchType is " + DataHostConfig.NOT_SWITCH_DS);
                return;
            }
            return;
        }

        AbstractPhysicalDBPool pool = this.source.getDbPool();
        pool.switchSourceIfNeed(this.source, reason);
        /*
        int curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
        // read node can't switch, only write node can switch
        if (pool.getWriteType() == PhysicalDBPool.WRITE_ONLYONE_NODE && !source.isReadNode()
            && curDatasourceHB != MySQLHeartbeat.OK_STATUS && pool.getSources().length > 1) {
            synchronized (pool) {
                // try to see if need switch datasource
                curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
                if (curDatasourceHB != MySQLHeartbeat.INIT_STATUS
                    && curDatasourceHB != MySQLHeartbeat.OK_STATUS) {
                    int curIndex = pool.getActiveIndex();
                    int nextId = pool.next(curIndex);
                    PhysicalDatasource[] allWriteNodes = pool.getSources();
                    while (true) {
                        if (nextId == curIndex) {
                            break;
                        }

                        PhysicalDatasource theSource = allWriteNodes[nextId];
                        MySQLHeartbeat theSourceHB = theSource.getHeartbeat();
                        int theSourceHBStatus = theSourceHB.getStatus();
                        if (theSourceHBStatus == MySQLHeartbeat.OK_STATUS) {
                            if (switchType == DataHostConfig.SYN_STATUS_SWITCH_DS) {
                                if (Integer.valueOf(0).equals(theSourceHB.getSlaveBehindMaster())) {
                                    logger.info("try to switch datasource, slave is synchronized to master " + theSource.getConfig());
                                    pool.switchSource(nextId, true, reason);
                                    break;
                                } else {
                                    logger.warn("ignored  datasource ,slave is not  synchronized to master , slave behind master :"
                                            + theSourceHB.getSlaveBehindMaster()
                                            + " " + theSource.getConfig());
                                }
                            } else {
                                // normal switch
                                logger.info("try to switch datasource ,not checked slave synchronize status " + theSource.getConfig());
                                pool.switchSource(nextId, true, reason);
                                break;
                            }
                        }
                        nextId = pool.next(nextId);
                    }
                }
            }
            } */
    }
}
