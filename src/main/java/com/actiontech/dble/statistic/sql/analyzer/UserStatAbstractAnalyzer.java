/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.analyzer;

import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * UserStatAnalyzer
 *
 * @author Ben
 */
public final class UserStatAbstractAnalyzer implements AbstractAnalyzer {

    private ConcurrentHashMap<String, UserStat> userStatMap = new ConcurrentHashMap<>();

    private static final UserStatAbstractAnalyzer INSTANCE = new UserStatAbstractAnalyzer();

    private UserStatAbstractAnalyzer() {
    }

    public static UserStatAbstractAnalyzer getInstance() {
        return INSTANCE;
    }

    @Override
    public void toAnalyzing(final StatisticFrontendSqlEntry fEntry) {
        UserStat userStat;
        if ((userStat = userStatMap.get(fEntry.getFrontend().getUser())) == null) {
            userStat = new UserStat(fEntry.getFrontend());
            userStatMap.put(fEntry.getFrontend().getUser(), userStat);
        }
        userStat.update(fEntry);
    }

    public Map<String, UserStat> getUserStatMap() {
        Map<String, UserStat> map = new ConcurrentHashMap<>(userStatMap.size());
        map.putAll(userStatMap);
        return map;
    }

    public void reset() {
        userStatMap = new ConcurrentHashMap<>();
    }

}
