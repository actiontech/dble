/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.lmax.disruptor.EventHandler;

public interface StatisticDataHandler extends EventHandler<StatisticEvent> {

    @Override
    void onEvent(StatisticEvent statisticEvent, long l, boolean b) throws Exception;

    Object getList();

    void clear();
}
