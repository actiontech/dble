/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ChildTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.WhereUnit;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.sqlengine.mpp.IsValue;
import com.actiontech.dble.sqlengine.mpp.RangeValue;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.stat.TableStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class ConditionUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionUtil.class);

    private ConditionUtil() {
    }

    private static void pruningConditions(List<WhereUnit> whereUnits, Map<String, String> tableAliasMap, String defaultSchema) {
        Iterator<WhereUnit> whereUnitIterator = whereUnits.listIterator();
        while (whereUnitIterator.hasNext()) {
            WhereUnit whereUnit = whereUnitIterator.next();
            String whereUnitContent = "empty";
            if (LOGGER.isTraceEnabled()) {
                whereUnitContent = whereUnit.toString();
            }
            final int subWhereSize = whereUnit.getSubWhereUnit().size();
            pruningConditions(whereUnit.getSubWhereUnit(), tableAliasMap, defaultSchema);
            final int subWhereSizeAfter = whereUnit.getSubWhereUnit().size();
            boolean orContainsEmpty = false;
            final int orSize = whereUnit.getOrConditionList().size();
            for (List<TableStat.Condition> conditions : whereUnit.getOrConditionList()) {
                pruningAndConditions(tableAliasMap, defaultSchema, conditions.listIterator());
                if (conditions.size() == 0) {
                    orContainsEmpty = true;
                    break;
                }
            }
            if (orContainsEmpty) {
                whereUnit.getOrConditionList().clear();
            }
            final int orSizeAfter = whereUnit.getOrConditionList().size();
            List<TableStat.Condition> outConditions = whereUnit.getOutAndConditions(); //outConditions item operator with AND
            ListIterator<TableStat.Condition> iteratorOutConditions = outConditions.listIterator();
            pruningAndConditions(tableAliasMap, defaultSchema, iteratorOutConditions);
            if (outConditions.size() == 0 && (subWhereSize != 0 && subWhereSizeAfter == 0) || (orSize != 0 && orSizeAfter == 0) || (subWhereSize == 0 && orSize == 0)) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("whereUnit [" + whereUnitContent + "] will be pruned for contains useless or condition");
                }
                whereUnitIterator.remove();
            }
        }

    }

    private static void pruningAndConditions(Map<String, String> tableAliasMap, String defaultSchema, ListIterator<TableStat.Condition> iteratorConditions) {
        while (iteratorConditions.hasNext()) {
            TableStat.Condition condition = iteratorConditions.next();
            List<Object> values = condition.getValues();
            if (values.size() == 0 || !checkConditionValues(values)) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("condition [" + condition + "] will be pruned for empty values");
                }
                iteratorConditions.remove(); //AND CONDITION can be pruned
            } else {
                TableStat.Condition newCondition = getUsefulCondition(condition, tableAliasMap, defaultSchema);
                if (newCondition == null) {
                    iteratorConditions.remove(); //AND CONDITION can be pruned
                } else {
                    iteratorConditions.set(newCondition); //replace table name and column name
                }
            }
        }
    }

    private static TableStat.Condition getUsefulCondition(TableStat.Condition condition, Map<String, String> tableAliasMap, String defaultSchema) {
        Pair<String, String> table = getTrueTableName(condition, tableAliasMap, defaultSchema);
        if (table == null) return null;

        String schemaName = table.getKey();
        if (schemaName == null) {
            return null;
        }
        String tableName = table.getValue();
        String tableFullName = schemaName + "." + tableName;

        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
        if (SchemaUtil.MYSQL_SYS_SCHEMA.contains(schemaName.toUpperCase()) || schemaConfig == null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("condition [" + condition + "] will be pruned for schema name " + schemaName.toUpperCase());
            }
            return null;
        }
        BaseTableConfig tableConfig = schemaConfig.getTables().get(tableName);
        if (tableConfig == null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("condition [" + condition + "] will be pruned for table is not config " + tableName);
            }
            return null;
        }

        String operator = condition.getOperator();
        //execute only between ,in and = is
        if (!operator.equalsIgnoreCase("between") && !operator.equals("=") && !operator.equalsIgnoreCase("in") && !operator.equalsIgnoreCase("IS")) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("condition [" + condition + "] will be pruned for operator is not [between,=,in,IS]");
            }
            return null;
        }
        if (tableConfig instanceof ShardingTableConfig) {
            String partitionCol = ((ShardingTableConfig) tableConfig).getShardingColumn();
            String columnName = StringUtil.removeBackQuote(condition.getColumn().getName().toUpperCase());
            if (columnName.equals(partitionCol)) {
                return genNewCondition(tableFullName, columnName, operator, condition.getValues());
            }
        }

        if (tableConfig instanceof ChildTableConfig) {
            String joinColumn = ((ChildTableConfig) tableConfig).getJoinColumn();
            String columnName = StringUtil.removeBackQuote(condition.getColumn().getName().toUpperCase());
            if (joinColumn != null && columnName.equals(joinColumn)) {
                return genNewCondition(tableFullName, columnName, operator, condition.getValues());
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("condition [" + condition + "] will be pruned for columnName is not shardingcolumn/joinkey");
        }
        return null;
    }

    private static Pair<String, String> getTrueTableName(TableStat.Condition condition, Map<String, String> tableAliasMap, String defaultSchema) {
        String tableFullName = condition.getColumn().getTable();
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tableFullName = tableFullName.toLowerCase();
        }
        if (tableAliasMap != null && tableAliasMap.get(tableFullName) == null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("condition [" + condition + "] will be pruned for can't find table " + tableFullName);
            }
            //ignore subQuery's alias
            return null;
        }

        Pair<String, String> table = getTableInfo(tableAliasMap, tableFullName, defaultSchema);
        return table;
    }

    private static Pair<String, String> getTableInfo(Map<String, String> tableAliasMap, String tableFullName, String defaultSchema) {
        if (tableAliasMap != null && tableAliasMap.get(tableFullName) != null &&
                !tableAliasMap.get(tableFullName).equals(tableFullName)) {
            tableFullName = tableAliasMap.get(tableFullName);
        }
        String schemaName;
        String tableName;
        int pos = tableFullName.indexOf(".");
        if (pos > 0) {
            tableName = tableFullName.substring(pos + 1);
            schemaName = tableFullName.substring(0, pos);
        } else {
            schemaName = defaultSchema;
            tableName = tableFullName;
        }
        return new Pair<>(schemaName, tableName);
    }

    private static TableStat.Condition genNewCondition(String tableName, String columnName, String operator, List<Object> values) {
        TableStat.Column column = new TableStat.Column(tableName, columnName);
        TableStat.Condition condition = new TableStat.Condition(column, operator);
        for (Object value : values) {
            condition.addValue(value);
        }
        return condition;
    }

    private static List<RouteCalculateUnit> mergedConditions(List<WhereUnit> storedWhereUnits) {
        if (storedWhereUnits.size() == 0) {
            return new ArrayList<>();
        }
        List<List<RouteCalculateUnit>> lstUnit = new ArrayList<>();
        for (WhereUnit whereUnit : storedWhereUnits) {
            lstUnit.add(mergeSubConditionWithOuterCondition(whereUnit));
        }

        return getMergedConditionList(lstUnit);
    }

    /**
     * mergeSubConditionWithOuterCondition
     * Only subWhereUnit will be deal
     */
    private static List<RouteCalculateUnit> mergeSubConditionWithOuterCondition(WhereUnit whereUnit) {
        List<RouteCalculateUnit> routeUnits = new ArrayList<>();
        if (whereUnit.getSubWhereUnit().size() > 0) {
            List<List<RouteCalculateUnit>> lstSubUnit = new ArrayList<>();
            for (WhereUnit sub : whereUnit.getSubWhereUnit()) {
                lstSubUnit.add(mergeSubConditionWithOuterCondition(sub));
            }
            routeUnits.addAll(getMergedConditionList(lstSubUnit));

        }
        routeUnits.addAll(conditionsToRouteUnits(whereUnit.getOrConditionList()));

        if (whereUnit.getOutAndConditions().size() > 0) {
            if (routeUnits.size() == 0) {
                routeUnits.add(new RouteCalculateUnit());
            }
            for (RouteCalculateUnit routeUnit : routeUnits) {
                conditionToRouteUnit(routeUnit, whereUnit.getOutAndConditions());
            }
        }
        return routeUnits;
    }


    private static List<RouteCalculateUnit> getMergedConditionList(List<List<RouteCalculateUnit>> routeUnits) {
        List<RouteCalculateUnit> mergedRouteUnitList = new ArrayList<>();
        if (routeUnits.size() == 0) {
            return mergedRouteUnitList;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("changeAndToOr will start ");
        }
        for (List<RouteCalculateUnit> routeUnit : routeUnits) {
            StringBuilder sb = new StringBuilder();
            if (LOGGER.isTraceEnabled()) {
                sb.append("changeAndToOr from [").append(mergedRouteUnitList).append("] and [").append(routeUnit).append("] merged to ");
            }
            mergedRouteUnitList = changeAndToOr(mergedRouteUnitList, routeUnit);
            if (LOGGER.isTraceEnabled()) {
                sb.append(mergedRouteUnitList);
                LOGGER.trace(sb.toString());
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("changeAndToOr end ");
        }
        return mergedRouteUnitList;
    }

    private static List<RouteCalculateUnit> changeAndToOr(List<RouteCalculateUnit> list1, List<RouteCalculateUnit> list2) {
        if (list1.size() == 0) {
            return list2;
        } else if (list2.size() == 0) {
            return list1;
        }

        List<RouteCalculateUnit> retList = new ArrayList<>();
        boolean containsAlwaysFalse = false;
        for (RouteCalculateUnit item1 : list1) {
            if (item1.isAlwaysFalse()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("this RouteCalculateUnit " + item1 + " is always false, so this Unit will be ignore for changeAndToOr");
                }
                containsAlwaysFalse = true;
                continue;
            }
            for (RouteCalculateUnit item2 : list2) {
                if (item2.isAlwaysFalse()) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("this RouteCalculateUnit " + item2 + " is always false, so this Unit will be ignore for changeAndToOr");
                    }
                    containsAlwaysFalse = true;
                    continue;
                }
                RouteCalculateUnit tmp = item1.merge(item2);
                if (tmp.isAlwaysFalse()) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("this RouteCalculateUnit " + tmp + " is always false, so this Unit will be ignore for changeAndToOr");
                    }
                    containsAlwaysFalse = true;
                    continue;
                }
                retList.add(tmp);
            }
        }
        if (retList.size() == 0 && containsAlwaysFalse) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            routeCalculateUnit.setAlwaysFalse(true);
            retList.add(routeCalculateUnit);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("changeAndToOr are all always false, so leave one alwaysFalse as RouteCalculateUnit");
            }
        }
        return retList;
    }


    public static void extendConditionsFromRelations(List<TableStat.Condition> conds, Set<TableStat.Relationship> relations) {
        List<TableStat.Condition> newConds = new ArrayList<>();
        Iterator<TableStat.Condition> iterator = conds.iterator();
        while (iterator.hasNext()) {
            TableStat.Condition condition = iterator.next();
            if (condition.getValues().size() == 0) {
                iterator.remove();
                continue;
            }
            if (!condition.getOperator().equals("=") && !condition.getOperator().equals("<=>")) {
                continue;
            }
            TableStat.Column column = condition.getColumn();
            for (TableStat.Relationship relation : relations) {
                if (!condition.getOperator().equalsIgnoreCase(relation.getOperator())) {
                    continue;
                }
                if (column.equals(relation.getLeft())) {
                    TableStat.Condition cond = new TableStat.Condition(relation.getRight(), condition.getOperator());
                    cond.getValues().addAll(condition.getValues());
                    newConds.add(cond);
                } else if (column.equals(relation.getRight())) {
                    TableStat.Condition cond = new TableStat.Condition(relation.getLeft(), condition.getOperator());
                    cond.getValues().addAll(condition.getValues());
                    newConds.add(cond);
                }
            }
        }
        conds.addAll(newConds);
    }

    private static List<RouteCalculateUnit> conditionsToRouteUnits(List<List<TableStat.Condition>> orConditionList) {
        List<RouteCalculateUnit> retList = new ArrayList<>();
        //find partition column in condition
        for (List<TableStat.Condition> andConditionList : orConditionList) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            conditionToRouteUnit(routeCalculateUnit, andConditionList);
            retList.add(routeCalculateUnit);
        }
        if (LOGGER.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (RouteCalculateUnit routeUnit : retList) {
                i++;
                sb.append("{ RouteCalculateUnit ").append(i).append(" :");
                sb.append(routeUnit.toString());
                sb.append("}");
            }
            LOGGER.trace(sb.toString());
        }
        return retList;
    }

    private static void conditionToRouteUnit(RouteCalculateUnit routeCalculateUnit, List<TableStat.Condition> andConditionList) {
        for (TableStat.Condition condition : andConditionList) {
            List<Object> values = condition.getValues();
            String columnName = condition.getColumn().getName();
            String tableFullName = condition.getColumn().getTable();
            String operator = condition.getOperator();
            String[] tableInfo = tableFullName.split("\\.");
            Pair<String, String> table = new Pair<>(tableInfo[0], tableInfo[1]);
            //execute only between ,in and =
            if (operator.equalsIgnoreCase("between")) {
                RangeValue rv = new RangeValue(values.get(0), values.get(1));
                routeCalculateUnit.addShardingExpr(table, columnName, rv);
            } else if (operator.equals("=")) {
                routeCalculateUnit.addShardingExpr(table, columnName, values.get(0));
            } else if (operator.equalsIgnoreCase("in")) {
                routeCalculateUnit.addShardingExpr(table, columnName, values.toArray());
            } else if (operator.equalsIgnoreCase("IS")) {
                IsValue isValue = new IsValue(values.get(0));
                routeCalculateUnit.addShardingExpr(table, columnName, isValue);
            }
        }
    }

    private static boolean checkConditionValues(List<Object> values) {
        for (Object value : values) {
            if (value != null) {
                return true;
            }
        }
        return false;
    }


    public static List<RouteCalculateUnit> buildRouteCalculateUnits(List<WhereUnit> whereUnits, Map<String, String> tableAliasMap, String defaultSchema) {
        if (LOGGER.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("these conditions will try to pruning:");
            int i = 0;
            sb.append("{");
            for (WhereUnit whereUnit : whereUnits) {
                if (i > 0) {
                    sb.append(" and ");
                }
                sb.append("(");
                sb.append(whereUnit.toString());
                sb.append(")");
                i++;
            }
            sb.append("}");
            LOGGER.trace(sb.toString());
        }
        ConditionUtil.pruningConditions(whereUnits, tableAliasMap, defaultSchema);
        if (whereUnits.size() == 0) {
            WhereUnit whereUnit = new WhereUnit();
            whereUnit.setFinishedParse(true);
            List<List<TableStat.Condition>> retList = new ArrayList<>();
            retList.add(new ArrayList<>());
            whereUnit.setOrConditionList(retList);
            whereUnits.add(whereUnit);
        }
        return ConditionUtil.mergedConditions(whereUnits);
    }
}
