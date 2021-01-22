package com.actiontech.dble.statistic.backend;

import com.lmax.disruptor.EventHandler;

public interface StatisticDataHandler extends EventHandler<Event> {

    @Override
    void onEvent(Event event, long l, boolean b) throws Exception;

    Object getList();

    void clear();
}
