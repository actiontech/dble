/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
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
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class MySQLHeartbeat {

    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLHeartbeat.class);
    public static final int DB_SYN_ERROR = -1;
    public static final int DB_SYN_NORMAL = 1;

    public static final int OK_STATUS = 1;
    public static final int ERROR_STATUS = -1;
    public static final int TIMEOUT_STATUS = -2;
    public static final int INIT_STATUS = 0;
    private static final long DEFAULT_HEARTBEAT_TIMEOUT = 30 * 1000L;
    private static final int DEFAULT_HEARTBEAT_RETRY = 10;
    // heartbeat config
    private static final int MAX_RETRY_COUNT = 5;
    protected long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;
    protected int heartbeatRetry = DEFAULT_HEARTBEAT_RETRY; // retry times after first error of heartbeat
    protected String heartbeatSQL;
    protected final AtomicBoolean isStop = new AtomicBoolean(true);
    protected final AtomicBoolean isChecking = new AtomicBoolean(false);
    protected int errorCount;
    protected volatile int status;
    protected final HeartbeatRecorder recorder = new HeartbeatRecorder();
    protected final DataSourceSyncRecorder asyncRecorder = new DataSourceSyncRecorder();

    private volatile Integer slaveBehindMaster;
    private volatile int dbSynStatus = DB_SYN_NORMAL;
    private final MySQLDataSource source;

    private final ReentrantLock lock;
    private final int maxRetryCount;

    private MySQLDetector detector;

    public MySQLHeartbeat(MySQLDataSource source) {
        this.source = source;
        this.lock = new ReentrantLock(false);
        this.maxRetryCount = MAX_RETRY_COUNT;
        this.status = INIT_STATUS;
        this.heartbeatSQL = source.getHostConfig().getHearbeatSQL();
    }

    public MySQLDataSource getSource() {
        return source;
    }


    public long getTimeout() {
        if (detector == null) {
            return -1L;
        }
        return detector.getHeartbeatTimeout();
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
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            isStop.compareAndSet(true, false);
            this.status = OK_STATUS;
        } finally {
            reentrantLock.unlock();
        }
    }

    public void stop() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            if (isStop.compareAndSet(false, true)) {
                if (!isChecking.get()) {
                    if (detector != null) {
                        detector.quit();
                        isChecking.set(false);
                    }
                }
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * execute heart beat
     */
    public void heartbeat() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
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
                        isChecking.compareAndSet(true, false);
                    } else if (detector.isHeartbeatTimeout()) {
                        setResult(TIMEOUT_STATUS);
                    }
                }
            }
        } finally {
            reentrantLock.unlock();
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
        if (this.status != OK_STATUS) {
            Map<String, String> labels = AlertUtil.genSingleLabel("data_host", this.source.getHostConfig().getName() + "-" + this.source.getConfig().getHostName());
            AlertUtil.alertResolve(AlarmCode.HEARTBEAT_FAIL, Alert.AlertLevel.WARN, "mysql", this.source.getConfig().getId(), labels);
        }
        switch (status) {
            case TIMEOUT_STATUS:
                this.status = INIT_STATUS;
                this.errorCount = 0;
                if (isStop.get()) {
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
        }
        if (isStop.get()) {
            detector.quit();
        }
    }

    private void setError() {
        // should continues check error status
        if (++errorCount < maxRetryCount) {
            if (detector != null && !detector.isQuit()) {
                heartbeat(); // error count not enough, heart beat again
            }
        } else {
            if (detector != null) {
                detector.quit();
            }
            this.status = ERROR_STATUS;
            this.errorCount = 0;
        }
    }

    private void setTimeout() {
        this.isChecking.set(false);
        status = TIMEOUT_STATUS;
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

        PhysicalDBPool pool = this.source.getDbPool();
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

    public Integer getSlaveBehindMaster() {
        return slaveBehindMaster;
    }

    public int getDbSynStatus() {
        return dbSynStatus;
    }

    public void setDbSynStatus(int dbSynStatus) {
        this.dbSynStatus = dbSynStatus;
    }

    public void setSlaveBehindMaster(Integer slaveBehindMaster) {
        this.slaveBehindMaster = slaveBehindMaster;
    }

    public int getStatus() {
        return status;
    }

    public boolean isChecking() {
        return isChecking.get();
    }

    public boolean isStop() {
        return isStop.get();
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

    public void setHeartbeatTimeout(long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public int getHeartbeatRetry() {
        return heartbeatRetry;
    }

    public void setHeartbeatRetry(int heartbeatRetry) {
        this.heartbeatRetry = heartbeatRetry;
    }

    public String getHeartbeatSQL() {
        return heartbeatSQL;
    }

    public void setHeartbeatSQL(String heartbeatSQL) {
        this.heartbeatSQL = heartbeatSQL;
    }

    public boolean isNeedHeartbeat() {
        return heartbeatSQL != null;
    }

    public DataSourceSyncRecorder getAsyncRecorder() {
        return this.asyncRecorder;
    }
}
