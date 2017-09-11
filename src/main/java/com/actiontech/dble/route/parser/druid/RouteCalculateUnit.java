/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.sqlengine.mpp.ColumnRoutePair;
import com.actiontech.dble.sqlengine.mpp.RangeValue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * RouteCalculateUnit
 *
 * @author wang.dw
 * @version 0.1.0
 * @date 2015-3-14 18:24:54
 * @copyright wonhigh.cn
 */
public class RouteCalculateUnit {
    private Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<>();

    public Map<String, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() {
        return tablesAndConditions;
    }

    public void addShardingExpr(String tableName, String columnName, Object value) {
        Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions.get(tableName);

        if (value == null) {
            // where a=null
            return;
        }

        if (tableColumnsMap == null) {
            tableColumnsMap = new LinkedHashMap<>();
            tablesAndConditions.put(tableName, tableColumnsMap);
        }

        String uperColName = columnName.toUpperCase();
        Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

        if (columValues == null) {
            columValues = new LinkedHashSet<>();
            tablesAndConditions.get(tableName).put(uperColName, columValues);
        }

        if (value instanceof Object[]) {
            for (Object item : (Object[]) value) {
                if (item == null) {
                    continue;
                }
                columValues.add(new ColumnRoutePair(item.toString()));
            }
        } else if (value instanceof RangeValue) {
            columValues.add(new ColumnRoutePair((RangeValue) value));
        } else {
            columValues.add(new ColumnRoutePair(value.toString()));
        }
    }

    public void clear() {
        tablesAndConditions.clear();
    }


}
