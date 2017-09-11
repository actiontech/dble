/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.server.parser.ServerParse;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * UserStatAnalyzer
 *
 * @author Ben
 */
public final class UserStatAnalyzer implements QueryResultListener {

    private LinkedHashMap<String, UserStat> userStatMap = new LinkedHashMap<>();

    private static final UserStatAnalyzer INSTANCE = new UserStatAnalyzer();

    private UserStatAnalyzer() {
    }

    public static UserStatAnalyzer getInstance() {
        return INSTANCE;
    }

    @Override
    public void onQueryResult(QueryResult query) {
        switch (query.getSqlType()) {
            case ServerParse.SELECT:
            case ServerParse.UPDATE:
            case ServerParse.INSERT:
            case ServerParse.DELETE:
            case ServerParse.REPLACE:
                String user = query.getUser();
                int sqlType = query.getSqlType();
                String sql = query.getSql();
                long sqlRows = query.getSqlRows();
                long netInBytes = query.getNetInBytes();
                long netOutBytes = query.getNetOutBytes();
                long startTime = query.getStartTime();
                long endTime = query.getEndTime();
                int resultSetSize = query.getResultSize();
                UserStat userStat = userStatMap.get(user);
                if (userStat == null) {
                    userStat = new UserStat(user);
                    userStatMap.put(user, userStat);
                }
                userStat.update(sqlType, sql, sqlRows, netInBytes, netOutBytes, startTime, endTime, resultSetSize);
                break;
            default:
                break;
        }
    }

    public Map<String, UserStat> getUserStatMap() {
        Map<String, UserStat> map = new LinkedHashMap<>(userStatMap.size());
        map.putAll(userStatMap);
        return map;
    }

    public void reset() {
        userStatMap = new LinkedHashMap<>();
    }

}
