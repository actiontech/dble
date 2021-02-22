package com.actiontech.dble.statistic.sql.handler;

import com.actiontech.dble.statistic.sql.StatisticEvent;
import com.lmax.disruptor.EventHandler;

public interface StatisticDataHandler extends EventHandler<StatisticEvent> {

    @Override
    void onEvent(StatisticEvent statisticEvent, long l, boolean b) throws Exception;

    Object getList();

    void clear();
}
