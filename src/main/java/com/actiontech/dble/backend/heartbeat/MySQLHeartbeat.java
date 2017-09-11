/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.heartbeat;

import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.config.model.DataHostConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class MySQLHeartbeat extends DBHeartbeat {

    private static final int MAX_RETRY_COUNT = 5;
    public static final Logger LOGGER = LoggerFactory.getLogger(MySQLHeartbeat.class);

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

    public MySQLDetector getDetector() {
        return detector;
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
        long t = detector.getLasstReveivedQryTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(t));
    }

    public void start() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            isStop.compareAndSet(true, false);
            super.status = OK_STATUS;
        } finally {
            reentrantLock.unlock();
        }
    }

    public void stop() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            if (isStop.compareAndSet(false, true)) {
                if (isChecking.get()) {
                    // nothing
                } else {
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
                        LOGGER.warn(source.getConfig().toString(), e);
                        setResult(ERROR_STATUS, null);
                        return;
                    }
                } else {
                    detector.heartbeat();
                }
            } else {
                if (detector != null) {
                    if (detector.isQuit()) {
                        isChecking.compareAndSet(true, false);
                    } else if (detector.isHeartbeatTimeout()) {
                        setResult(TIMEOUT_STATUS, null);
                    }
                }
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    public void setResult(int result, String msg) {
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
            switchSourceIfNeed("heartbeat error");
        }
    }

    private void setOk() {
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
            && curDatasourceHB != DBHeartbeat.OK_STATUS && pool.getSources().length > 1) {
            synchronized (pool) {
                // try to see if need switch datasource
                curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
                if (curDatasourceHB != DBHeartbeat.INIT_STATUS
                    && curDatasourceHB != DBHeartbeat.OK_STATUS) {
                    int curIndex = pool.getActiveIndex();
                    int nextId = pool.next(curIndex);
                    PhysicalDatasource[] allWriteNodes = pool.getSources();
                    while (true) {
                        if (nextId == curIndex) {
                            break;
                        }

                        PhysicalDatasource theSource = allWriteNodes[nextId];
                        DBHeartbeat theSourceHB = theSource.getHeartbeat();
                        int theSourceHBStatus = theSourceHB.getStatus();
                        if (theSourceHBStatus == DBHeartbeat.OK_STATUS) {
                            if (switchType == DataHostConfig.SYN_STATUS_SWITCH_DS) {
                                if (Integer.valueOf(0).equals(theSourceHB.getSlaveBehindMaster())) {
                                    LOGGER.info("try to switch datasource, slave is synchronized to master " + theSource.getConfig());
                                    pool.switchSource(nextId, true, reason);
                                    break;
                                } else {
                                    LOGGER.warn("ignored  datasource ,slave is not  synchronized to master , slave behind master :"
                                            + theSourceHB.getSlaveBehindMaster()
                                            + " " + theSource.getConfig());
                                }
                            } else {
                                // normal switch
                                LOGGER.info("try to switch datasource ,not checked slave synchronize status " + theSource.getConfig());
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
