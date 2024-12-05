/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.handler;

import com.oceanbase.obsharding_d.statistic.sql.StatisticEvent;
import com.lmax.disruptor.EventHandler;

public interface StatisticDataHandler extends EventHandler<StatisticEvent> {

    @Override
    void onEvent(StatisticEvent statisticEvent, long l, boolean b) throws Exception;

    Object getList();

    void clear();
}
