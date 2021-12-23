/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * QueryResultDispatcher
 *
 * @author zhuam
 */
public final class QueryResultDispatcher {
    private QueryResultDispatcher() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryResultDispatcher.class);

    private static List<QueryResultListener> listeners = new CopyOnWriteArrayList<>();

    // load in int
    static {
        listeners.add(UserStatAnalyzer.getInstance());
        listeners.add(TableStatAnalyzer.getInstance());
        listeners.add(QueryConditionAnalyzer.getInstance());
    }

    public static void addListener(QueryResultListener listener) {
        if (listener == null) {
            throw new NullPointerException();
        }
        listeners.add(listener);
    }

    public static void removeListener(QueryResultListener listener) {
        listeners.remove(listener);
    }

    public static void removeAllListener() {
        listeners.clear();
    }

    public static void doSqlStat(final RouteResultset rrs, final NonBlockingSession session, long sqlRows, long netOutBytes, long resultSize) {
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            long netInBytes = 0;
            if (rrs.getStatement() != null) {
                netInBytes = rrs.getStatement().getBytes().length;
            }
            QueryResult queryResult = new QueryResult(session.getShardingService().getUser(), rrs.getSqlType(), rrs.getStatement(), sqlRows,
                    netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), resultSize);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to record sql:" + rrs.getStatement());
            }
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    public static void dispatchQuery(final QueryResult queryResult) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {

            public void run() {

                for (QueryResultListener listener : listeners) {
                    try {
                        listener.onQueryResult(queryResult);
                    } catch (Exception e) {
                        LOGGER.info("error:", e);
                    }
                }
            }
        });
    }

}
