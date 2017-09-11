/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.DbleServer;
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

    public static void dispatchQuery(final QueryResult queryResult) {
        DbleServer.getInstance().getBusinessExecutor().execute(new Runnable() {

            public void run() {

                for (QueryResultListener listener : listeners) {
                    try {
                        listener.onQueryResult(queryResult);
                    } catch (Exception e) {
                        LOGGER.error("error:", e);
                    }
                }
            }
        });
    }

}
