package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.TableByUserByEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;
import com.actiontech.dble.statistic.sql.handler.AssociateTablesByEntryByUserCalcHandler;
import com.actiontech.dble.statistic.sql.handler.FrontendByBackendByEntryByUserCalcHandler;
import com.actiontech.dble.statistic.sql.handler.StatisticDataHandler;
import com.actiontech.dble.statistic.sql.handler.TableByUserByEntryCalcHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public final class StatisticManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticManager.class);
    private static final StatisticManager INSTANCE = new StatisticManager();
    private StatisticDisruptor disruptor;
    private static Map<String, StatisticDataHandler> statisticDataHandlers = new HashMap<>(8);
    private static StatisticListener statisticListener = StatisticListener.getInstance();
    private boolean isStart = false;

    // variable
    private volatile boolean enable = SystemConfig.getInstance().getEnableStatistic() == 1;
    private volatile int associateTablesByEntryByUserTableSize = SystemConfig.getInstance().getAssociateTablesByEntryByUserTableSize();
    private volatile int frontendByBackendByEntryByUserTableSize = SystemConfig.getInstance().getFrontendByBackendByEntryByUserTableSize();
    private volatile int tableByUserByEntryTableSize = SystemConfig.getInstance().getTableByUserByEntryTableSize();
    private int statisticQueueSize = SystemConfig.getInstance().getStatisticQueueSize();

    private StatisticManager() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                close();
            }
        });
    }

    public static StatisticManager getInstance() {
        return INSTANCE;
    }

    static {
        statisticDataHandlers.put(FrontendByBackendByEntryByUser.TABLE_NAME, new FrontendByBackendByEntryByUserCalcHandler());
        statisticDataHandlers.put(TableByUserByEntry.TABLE_NAME, new TableByUserByEntryCalcHandler());
        statisticDataHandlers.put(AssociateTablesByEntryByUser.TABLE_NAME, new AssociateTablesByEntryByUserCalcHandler());
        // TODO other handler ....
    }

    // start
    public void start() {
        if (isStart) return;
        statisticDataHandlers.values().stream().forEach(StatisticDataHandler::clear);
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
        if (!isStart) return;
        statisticListener.stop();
        if (disruptor != null)
            disruptor.stop();
        isStart = false;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("stop sql statistic success");
        }
    }

    public void close() {
        stop();
        statisticDataHandlers.values().stream().forEach(StatisticDataHandler::clear);
    }

    // push
    public void push(final StatisticEntry entry) {
        disruptor.push(entry);
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
        if (enable) {
            start();
        } else {
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

    public int getStatisticQueueSize() {
        return statisticQueueSize;
    }

    public StatisticDataHandler getHandler(String key) {
        return statisticDataHandlers.get(key);
    }
}
