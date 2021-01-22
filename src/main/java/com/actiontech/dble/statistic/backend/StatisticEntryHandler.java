package com.actiontech.dble.statistic.backend;

import com.lmax.disruptor.WorkHandler;

public interface StatisticEntryHandler extends WorkHandler<Event> {

    @Override
    void onEvent(Event event) throws Exception;

    Object getList();
}
