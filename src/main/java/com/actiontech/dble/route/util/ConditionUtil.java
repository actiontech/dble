/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.WhereUnit;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.sqlengine.mpp.ColumnRoutePair;
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

        String schemaName = table.getKey();
        String tableName = table.getValue();
        tableFullName = schemaName + "." + tableName;
        if (SchemaUtil.MYSQL_SYS_SCHEMA.contains(schemaName.toUpperCase())) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("condition [" + condition + "] will be pruned for schema name " + schemaName.toUpperCase());
            }
            return null;
        }
        TableConfig tableConfig = DbleServer.getInstance().getConfig().getSchemas().get(schemaName).getTables().get(tableName);
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
        String partitionCol = tableConfig.getPartitionColumn();

        String columnName = StringUtil.removeBackQuote(condition.getColumn().getName().toUpperCase());
        if (columnName.equals(partitionCol)) {
            return genNewCondition(tableFullName, columnName, operator, condition.getValues());
        }

        String joinKey = tableConfig.getJoinKey();
        if (joinKey != null && columnName.equals(joinKey)) {
            return genNewCondition(tableFullName, columnName, operator, condition.getValues());
        }
        String catchKey = tableConfig.getCacheKey();
        if (catchKey != null && columnName.equals(catchKey)) {
            return genNewCondition(tableFullName, columnName, operator, condition.getValues());
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("condition [" + condition + "] will be pruned for columnName is not shardingcolumn/joinkey/cachekey");
        }
        return null;
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

    private static List<List<TableStat.Condition>> mergedConditions(List<WhereUnit> storedWhereUnits) {
        if (storedWhereUnits.size() == 0) {
            return new ArrayList<>();
        }

        for (WhereUnit whereUnit : storedWhereUnits) {
            mergeSubConditionWithOuterCondition(whereUnit);
        }

        return getMergedConditionList(storedWhereUnits);

    }

    /**
     * mergeSubConditionWithOuterCondition
     * Only subWhereUnit will be deal
     */
    private static void mergeSubConditionWithOuterCondition(WhereUnit whereUnit) {
        if (whereUnit.getSubWhereUnit().size() > 0) {
            for (WhereUnit sub : whereUnit.getSubWhereUnit()) {
                mergeSubConditionWithOuterCondition(sub);
            }
            List<List<TableStat.Condition>> mergedConditionList = getMergedConditionList(whereUnit.getSubWhereUnit());
            if (whereUnit.getOutAndConditions().size() > 0) {
                for (List<TableStat.Condition> mergedCondition : mergedConditionList) {
                    mergedCondition.addAll(whereUnit.getOutAndConditions());
                }
            }
            whereUnit.getOrConditionList().addAll(mergedConditionList);
        } else if (whereUnit.getOutAndConditions().size() > 0) {
            whereUnit.getOrConditionList().add(whereUnit.getOutAndConditions());
        }
    }

    private static List<List<TableStat.Condition>> getMergedConditionList(List<WhereUnit> whereUnitList) {
        List<List<TableStat.Condition>> mergedConditionList = new ArrayList<>();
        if (whereUnitList.size() == 0) {
            return mergedConditionList;
        }
        mergedConditionList.addAll(whereUnitList.get(0).getOrConditionList());

        for (int i = 1; i < whereUnitList.size(); i++) {
            mergedConditionList = merge(mergedConditionList, whereUnitList.get(i).getOrConditionList());
        }
        return mergedConditionList;
    }


    private static List<List<TableStat.Condition>> merge(List<List<TableStat.Condition>> list1, List<List<TableStat.Condition>> list2) {
        if (list1.size() == 0) {
            return list2;
        } else if (list2.size() == 0) {
            return list1;
        }

        List<List<TableStat.Condition>> retList = new ArrayList<>();
        for (List<TableStat.Condition> aList1 : list1) {
            for (List<TableStat.Condition> aList2 : list2) {
                List<TableStat.Condition> tmp = new ArrayList<>();
                tmp.addAll(aList1);
                tmp.addAll(aList2);
                retList.add(tmp);
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

    private static List<RouteCalculateUnit> transformConditionToRouteUnits(List<List<TableStat.Condition>> conditionList) {
        List<RouteCalculateUnit> retList = new ArrayList<>();
        //find partition column in condition
        for (List<TableStat.Condition> aConditionList : conditionList) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            for (TableStat.Condition condition : aConditionList) {
                List<Object> values = condition.getValues();
                String columnName = condition.getColumn().getName();
                String tableFullName = condition.getColumn().getTable();
                String operator = condition.getOperator();
                String[] tableInfo = tableFullName.split("\\.");
                Pair<String, String> table = new Pair<>(tableInfo[0], tableInfo[1]);
                //execute only between ,in and =
                if (operator.equalsIgnoreCase("between")) {
                    RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
                    routeCalculateUnit.addShardingExpr(table, columnName, rv);
                } else if (operator.equals("=") || operator.equalsIgnoreCase("in")) {
                    routeCalculateUnit.addShardingExpr(table, columnName, values.toArray());
                } else if (operator.equalsIgnoreCase("IS")) {
                    IsValue isValue = new IsValue(values.toArray());
                    routeCalculateUnit.addShardingExpr(table, columnName, isValue);
                }
            }
            retList.add(routeCalculateUnit);
        }
        if (LOGGER.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (RouteCalculateUnit routeUnit : retList) {
                i++;
                sb.append("{ RouteCalculateUnit ").append(i).append(" :");
                Map<Pair<String, String>, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
                if (tablesAndConditions != null) {
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
                }
                sb.append("}");
            }
            LOGGER.trace(sb.toString());
        }
        return retList;
    }

    private static boolean checkConditionValues(List<Object> values) {
        for (Object value : values) {
            if (value != null && !value.toString().equals("")) {
                return true;
            }
        }
        return false;
    }


    public static List<RouteCalculateUnit> buildRouteCalculateUnits(List<WhereUnit> whereUnits, Map<String, String> tableAliasMap, String defaultSchema) {
        ConditionUtil.pruningConditions(whereUnits, tableAliasMap, defaultSchema);
        if (whereUnits.size() == 0) {
            WhereUnit whereUnit = new WhereUnit();
            whereUnit.setFinishedParse(true);
            List<List<TableStat.Condition>> retList = new ArrayList<>();
            retList.add(new ArrayList<>());
            whereUnit.setOrConditionList(retList);
            whereUnits.add(whereUnit);
        }
        List<List<TableStat.Condition>> conditions = ConditionUtil.mergedConditions(whereUnits);
        return ConditionUtil.transformConditionToRouteUnits(conditions);
    }
}
