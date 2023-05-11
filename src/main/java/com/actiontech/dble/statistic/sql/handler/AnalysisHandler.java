package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.analyzer.AbstractAnalyzer;
import com.actiontech.dble.statistic.sql.analyzer.QueryConditionAnalyzer;
import com.actiontech.dble.statistic.sql.analyzer.TableStatAnalyzer;
import com.actiontech.dble.statistic.sql.analyzer.UserStatAbstractAnalyzer;
import com.actiontech.dble.statistic.sql.entry.StatisticEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * show @@sql.sum;/show @@sql.sum.user;
 * show @@sql.sum.table;
 * show @@sql.condition;
 */
public class AnalysisHandler implements StatisticDataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalysisHandler.class);

    private static List<AbstractAnalyzer> listeners = new CopyOnWriteArrayList<>();

    static {
        listeners.add(UserStatAbstractAnalyzer.getInstance());
        listeners.add(TableStatAnalyzer.getInstance());
        listeners.add(QueryConditionAnalyzer.getInstance());
    }

    @Override
    public void onEvent(StatisticEvent statisticEvent, long l, boolean b) throws Exception {
        if (!StatisticManager.getInstance().isEnableAnalysis()) {
            return;
        }
        StatisticEntry entry = statisticEvent.getEntry();
        if (entry instanceof StatisticFrontendSqlEntry) {
            StatisticFrontendSqlEntry frontendSqlEntry = (StatisticFrontendSqlEntry) entry;
            for (AbstractAnalyzer listener : listeners) {
                try {
                    listener.toAnalyzing(frontendSqlEntry);
                } catch (Exception e) {
                    LOGGER.info("analyze error:", e);
                }
            }
        }
    }

    @Override
    public Object getList() {
        return null;
    }

    @Override
    public void clear() {

    }
}
