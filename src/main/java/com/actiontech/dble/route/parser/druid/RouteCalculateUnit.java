/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.sqlengine.mpp.ColumnRoute;
import com.actiontech.dble.sqlengine.mpp.IsValue;
import com.actiontech.dble.sqlengine.mpp.RangeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RouteCalculateUnit
 */
public class RouteCalculateUnit {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouteCalculateUnit.class);
    private boolean alwaysFalse = false;
    private Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = new LinkedHashMap<>();

    public Map<Pair<String, String>, Map<String, ColumnRoute>> getTablesAndConditions() {
        return tablesAndConditions;
    }

    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public void setAlwaysFalse(boolean alwaysFalse) {
        this.alwaysFalse = alwaysFalse;
    }

    public void addShardingExpr(Pair<String, String> table, String columnName, Object value) {
        if (alwaysFalse) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("this RouteCalculateUnit is always false, ignore " + changeValueToColumnRoute(value));
            }
            return;
        }
        Map<String, ColumnRoute> tableColumnsMap = tablesAndConditions.get(table);

        if (value == null) {
            // where a=null
            return;
        }

        if (tableColumnsMap == null) {
            tableColumnsMap = new LinkedHashMap<>();
            tablesAndConditions.put(table, tableColumnsMap);
        }

        String upperColName = columnName.toUpperCase();
        ColumnRoute columnValue = tableColumnsMap.get(upperColName);

        if (columnValue == null) {
            columnValue = changeValueToColumnRoute(value);
            if (columnValue.isAlwaysFalse()) {
                markAlwaysFalse(columnValue);
                return;
            }
            tableColumnsMap.put(upperColName, columnValue);
        } else {
            ColumnRoute newColumnValue = changeValueToColumnRoute(value);
            if (newColumnValue.isAlwaysFalse()) {
                markAlwaysFalse(columnValue);
                return;
            }
            columnValue = mergeColumnRoute(columnValue, newColumnValue);
            if (columnValue.isAlwaysFalse()) {
                markAlwaysFalse(columnValue);
                return;
            }
            tableColumnsMap.put(upperColName, columnValue);
        }
    }

    private void markAlwaysFalse(ColumnRoute columnValue) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("this condition " + columnValue + " is always false, so this RouteCalculateUnit will be always false");
        }
        clear();
        alwaysFalse = true;
    }

    public RouteCalculateUnit merge(RouteCalculateUnit other) {
        RouteCalculateUnit ret = this.deepClone();
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> otherTables : other.tablesAndConditions.entrySet()) {
            Pair<String, String> otherTable = otherTables.getKey();
            Map<String, ColumnRoute> tableColumnsMap = ret.getTablesAndConditions().get(otherTable);
            if (tableColumnsMap == null) {
                tableColumnsMap = new LinkedHashMap<>();
                tableColumnsMap.putAll(otherTables.getValue());
                ret.tablesAndConditions.put(otherTable, tableColumnsMap);
            } else {
                for (Map.Entry<String, ColumnRoute> otherColumnPairs : otherTables.getValue().entrySet()) {
                    String upperColName = otherColumnPairs.getKey();
                    ColumnRoute otherColumnPair = otherColumnPairs.getValue();
                    ColumnRoute thisColumnPair = tableColumnsMap.get(upperColName);
                    if (thisColumnPair == null) {
                        tableColumnsMap.put(upperColName, otherColumnPair);
                    } else {
                        ColumnRoute columnValue = mergeColumnRoute(thisColumnPair, otherColumnPair);
                        if (columnValue.isAlwaysFalse()) {
                            ret.markAlwaysFalse(columnValue);
                            LOGGER.trace("this RouteCalculateUnit [" + this + "] and RouteCalculateUnit [" + other + "] merged to RouteCalculateUnit[" + ret + "]");
                            return ret;
                        }
                        tableColumnsMap.put(upperColName, columnValue);
                    }
                }
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("this RouteCalculateUnit [" + this + "] and RouteCalculateUnit [" + other + "] merged to RouteCalculateUnit[" + ret + "]");
        }
        return ret;
    }

    private RouteCalculateUnit deepClone() {
        RouteCalculateUnit obj = new RouteCalculateUnit();
        obj.setAlwaysFalse(this.alwaysFalse);
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : this.tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            Map<String, ColumnRoute> tableColumnsMap = new LinkedHashMap<>();
            tableColumnsMap.putAll(entry.getValue());
            obj.tablesAndConditions.put(table, tableColumnsMap);
        }
        return obj;
    }

    /**
     * oldItem may contains (in or =) and range
     * newItem contains  in or = or range(only one item)
     */
    private ColumnRoute mergeColumnRoute(ColumnRoute oldItem, ColumnRoute newItem) {
        ColumnRoute oldItemClone = oldItem.deepClone();
        ColumnRoute ret;
        if (newItem.getColValue() != null) {
            if (oldItemClone.getColValue() != null) { // = merge =
                if (!oldItemClone.getColValue().equals(newItem.getColValue())) {
                    ret = new ColumnRoute(true);
                } else {
                    ret = oldItemClone;
                }
            } else if (oldItemClone.getInValues() != null) { // in merge =
                if (!oldItemClone.getInValues().contains(newItem.getColValue())) {
                    ret = new ColumnRoute(true);
                } else { // in change to =
                    oldItemClone.setColValue(newItem.getColValue());
                    oldItemClone.setInValues(null);
                    ret = oldItemClone;
                }
            } else { // range merge =
                oldItemClone.setColValue(newItem.getColValue());
                ret = oldItemClone;
            }
        } else if (newItem.getInValues() != null) {
            if (oldItemClone.getColValue() != null) { // = merge in
                if (!newItem.getInValues().contains(oldItemClone.getColValue())) {
                    ret = new ColumnRoute(true);
                } else {
                    ret = oldItemClone;
                }
            } else if (oldItemClone.getInValues() != null) {  // in merge in
                boolean changed = oldItemClone.getInValues().retainAll(newItem.getInValues());
                if (changed && oldItemClone.getInValues().size() == 0) {
                    ret = new ColumnRoute(true);
                } else {
                    ret = oldItemClone;
                }
            } else { // range merge in
                oldItemClone.setInValues(newItem.getInValues());
                ret = oldItemClone;
            }
        } else if (newItem.getRangeValues() != null) {
            oldItemClone.addRangeValues(newItem.getRangeValues());
            ret = oldItemClone;
        } else {
            ret = new ColumnRoute(true); //WILL NOT HAPPEN
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("ColumnRoute[" + oldItem + "] and ColumnRoute[" + newItem + "] will merge to ColumnRoute[" + ret + "]");
        }
        return ret;
    }

    private ColumnRoute changeValueToColumnRoute(Object value) {
        ColumnRoute columnValue = null;
        if (value instanceof Object[]) { //in
            HashSet<Object> inValues = new HashSet<>();
            for (Object item : (Object[]) value) {
                if (item == null) {
                    columnValue = new ColumnRoute(true);
                    break;
                }
                inValues.add(item);
            }
            if (columnValue == null) {
                columnValue = new ColumnRoute(inValues);
            }
        } else if (value instanceof RangeValue) { //between
            columnValue = new ColumnRoute((RangeValue) value);
        } else if (value instanceof IsValue) { //is
            columnValue = new ColumnRoute((IsValue) value);
        } else { // =
            columnValue = new ColumnRoute(value);
        }
        return columnValue;
    }

    public void clear() {
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
            entry.getValue().clear();
        }
        tablesAndConditions.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            String schemaName = table.getKey();
            String tableName = table.getValue();
            Map<String, ColumnRoute> columnsMap = entry.getValue();
            for (Map.Entry<String, ColumnRoute> columns : columnsMap.entrySet()) {
                String columnName = columns.getKey();
                ColumnRoute pair = columns.getValue();
                sb.append("{").append("schema:").append(schemaName).append(",table:").append(tableName);
                sb.append(",column:").append(columnName).append(",value :[").append(pair.toString());
                sb.append("]},");
            }
        }
        return sb.toString();
    }
}
