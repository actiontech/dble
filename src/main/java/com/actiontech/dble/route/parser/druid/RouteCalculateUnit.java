/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.sqlengine.mpp.ColumnRoutePair;
import com.actiontech.dble.sqlengine.mpp.IsValue;
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
    private Map<Pair<String, String>, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<>();

    public Map<Pair<String, String>, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() {
        return tablesAndConditions;
    }

    public void addShardingExpr(Pair<String, String> table, String columnName, Object value) {
        Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions.get(table);

        if (value == null) {
            // where a=null
            return;
        }

        if (tableColumnsMap == null) {
            tableColumnsMap = new LinkedHashMap<>();
            tablesAndConditions.put(table, tableColumnsMap);
        }

        String upperColName = columnName.toUpperCase();
        Set<ColumnRoutePair> columnValues = tableColumnsMap.get(upperColName);

        if (columnValues == null) {
            columnValues = new LinkedHashSet<>();
            tablesAndConditions.get(table).put(upperColName, columnValues);
        }

        if (value instanceof Object[]) {
            for (Object item : (Object[]) value) {
                if (item == null) {
                    continue;
                }
                columnValues.add(new ColumnRoutePair(item.toString()));
            }
        } else if (value instanceof RangeValue) {
            columnValues.add(new ColumnRoutePair((RangeValue) value));
        } else if (value instanceof IsValue) {
            columnValues.add(new ColumnRoutePair((IsValue) value, true));
        } else {
            columnValues.add(new ColumnRoutePair(value.toString()));
        }
    }

    public void clear() {
        tablesAndConditions.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Pair<String, String>, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            String schemaName = table.getKey();
            String tableName = table.getValue();
            Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
            for (Map.Entry<String, Set<ColumnRoutePair>> columns : columnsMap.entrySet()) {
                String columnName = columns.getKey();
                Set<ColumnRoutePair> values = columns.getValue();
                for (ColumnRoutePair pair : values) {
                    if (pair.colValue != null) {
                        sb.append("{").append("schema:").append(schemaName).append(",table:").append(tableName);
                        sb.append(",column:").append(columnName).append(",value:").append(pair.colValue).append("},");
                    } else if (pair.rangeValue != null) {
                        sb.append("{").append("schema:").append(schemaName).append(",table:").append(tableName);
                        sb.append(",column:").append(columnName).append(",value between:").append(pair.rangeValue.getBeginValue());
                        sb.append("~").append(pair.rangeValue.getEndValue()).append("},");
                    }
                }
            }
        }
        return sb.toString();
    }
}
