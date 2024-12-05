/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.SqlLog;
import com.oceanbase.obsharding_d.services.manager.information.tables.statistic.TableByUserByEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticEntry;
import com.oceanbase.obsharding_d.statistic.sql.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class StatisticManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticManager.class);
    private static final StatisticManager INSTANCE = new StatisticManager();
    private StatisticDisruptor disruptor;
    private static Map<String, StatisticDataHandler> statisticDataHandlers = new HashMap<>(8);
    private static StatisticListener statisticListener = StatisticListener.getInstance();

    private static ConcurrentLinkedQueue<UsageDataBlock> usageData = new ConcurrentLinkedQueue<>();
    private boolean isStart = false;
    private Timer queueMonitor;
    private static final ReentrantReadWriteLock MONITO_RLOCK = new ReentrantReadWriteLock();

    // variable
    private volatile boolean enable = SystemConfig.getInstance().getEnableStatistic() == 1;
    private volatile int associateTablesByEntryByUserTableSize = SystemConfig.getInstance().getAssociateTablesByEntryByUserTableSize();
    private volatile int frontendByBackendByEntryByUserTableSize = SystemConfig.getInstance().getFrontendByBackendByEntryByUserTableSize();
    private volatile int tableByUserByEntryTableSize = SystemConfig.getInstance().getTableByUserByEntryTableSize();
    private int statisticQueueSize = SystemConfig.getInstance().getStatisticQueueSize();

    // sampling
    private volatile int sqlLogSize = SystemConfig.getInstance().getSqlLogTableSize();
    private volatile int samplingRate = SystemConfig.getInstance().getSamplingRate();

    private StatisticManager() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    public static StatisticManager getInstance() {
        return INSTANCE;
    }

    static {
        statisticDataHandlers.put(FrontendByBackendByEntryByUser.TABLE_NAME, new FrontendByBackendByEntryByUserCalcHandler());
        statisticDataHandlers.put(TableByUserByEntry.TABLE_NAME, new TableByUserByEntryCalcHandler());
        statisticDataHandlers.put(AssociateTablesByEntryByUser.TABLE_NAME, new AssociateTablesByEntryByUserCalcHandler());
        // sampling
        statisticDataHandlers.put(SqlLog.TABLE_NAME, new SqlStatisticHandler());
    }

    // start
    public void start() {
        statisticDataHandlers.values().forEach(StatisticDataHandler::clear);
        ArrayList list = new ArrayList<>(statisticDataHandlers.values());
        disruptor = new StatisticDisruptor(statisticQueueSize, (StatisticDataHandler[]) list.toArray(new StatisticDataHandler[list.size()]));
        statisticListener.start();
        isStart = true;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("start sql statistic success");
        }
    }

    // stop
    public void stop() {
        statisticListener.stop();
        if (disruptor != null) {
            disruptor.stop();
            disruptor = null;
        }
        isStart = false;
        cancelMonitoring();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("stop sql statistic success");
        }
    }

    public void close() {
        stop();
        statisticDataHandlers.values().forEach(StatisticDataHandler::clear);
    }

    // push
    public void push(final StatisticEntry entry) {
        disruptor.push(entry);
    }

    public boolean isMonitoring() {
        MONITO_RLOCK.readLock().lock();
        try {
            return queueMonitor != null;
        } finally {
            MONITO_RLOCK.readLock().unlock();
        }
    }

    public Timer getQueueMonitor() {
        MONITO_RLOCK.writeLock().lock();
        try {
            if (disruptor == null)
                return null;
            if (queueMonitor == null)
                queueMonitor = new Timer("monitorStatisticQueue");
            return queueMonitor;
        } finally {
            MONITO_RLOCK.writeLock().unlock();
        }
    }

    public void cancelMonitoring() {
        MONITO_RLOCK.writeLock().lock();
        try {
            if (null != queueMonitor) {
                queueMonitor.cancel();
                queueMonitor = null;
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("cancel queue monitor");
            }
        } finally {
            MONITO_RLOCK.writeLock().unlock();
        }
    }

    public ConcurrentLinkedQueue<UsageDataBlock> getUsageData() {
        return usageData;
    }

    public void resetUsageData() {
        usageData.clear();
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
        if (enable && !isStart) {
            start();
            return;
        }
        if (!enable && (isStart && samplingRate == 0)) {
            stop();
        }
    }

    public int getAssociateTablesByEntryByUserTableSize() {
        return associateTablesByEntryByUserTableSize;
    }

    public void setAssociateTablesByEntryByUserTableSize(int associateTablesByEntryByUserTableSize) {
        this.associateTablesByEntryByUserTableSize = associateTablesByEntryByUserTableSize;
    }

    public int getFrontendByBackendByEntryByUserTableSize() {
        return frontendByBackendByEntryByUserTableSize;
    }

    public void setFrontendByBackendByEntryByUserTableSize(int frontendByBackendByEntryByUserTableSize) {
        this.frontendByBackendByEntryByUserTableSize = frontendByBackendByEntryByUserTableSize;
    }

    public int getTableByUserByEntryTableSize() {
        return tableByUserByEntryTableSize;
    }

    public void setTableByUserByEntryTableSize(int tableByUserByEntryTableSize) {
        this.tableByUserByEntryTableSize = tableByUserByEntryTableSize;
    }

    public int getSqlLogSize() {
        return sqlLogSize;
    }

    public void setSqlLogSize(int sqlLogSize) {
        this.sqlLogSize = sqlLogSize;
    }

    public void setSamplingRate(int samplingRate) {
        this.samplingRate = samplingRate;
        if (samplingRate > 0) {
            final SqlStatisticHandler handler = ((SqlStatisticHandler) statisticDataHandlers.get(SqlLog.TABLE_NAME));
            handler.setSampleDecisions(samplingRate);
            if (!isStart) {
                start();
            }
            return;
        }
        if (samplingRate == 0 && (isStart && !enable)) {
            stop();
        }
    }

    public int getSamplingRate() {
        return samplingRate;
    }

    public int getStatisticQueueSize() {
        return statisticQueueSize;
    }

    public long getDisruptorRemaining() {
        return disruptor.getDisruptor().getRingBuffer().remainingCapacity();
    }

    public StatisticDataHandler getHandler(String key) {
        return statisticDataHandlers.get(key);
    }
}
