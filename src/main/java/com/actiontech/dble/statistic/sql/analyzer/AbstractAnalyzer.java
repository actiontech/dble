/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.analyzer;

import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;

public interface AbstractAnalyzer {

    void toAnalyzing(StatisticFrontendSqlEntry fEntry);

}
