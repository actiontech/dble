/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.config.model.rule.RuleConfig;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.util.SplitUtil;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author mycat
 */
public class TableConfig {
    public enum TableTypeEnum {
        TYPE_SHARDING_TABLE, TYPE_GLOBAL_TABLE
    }

    private final String name;
    private final String incrementColumn;
    private final int maxLimit;
    private final TableTypeEnum tableType;
    private final ArrayList<String> shardingNodes;
    private final RuleConfig rule;
    private final String partitionColumn;
    private final boolean sqlRequiredSharding;
    private final boolean isNoSharding;
    private final boolean globalCheck;
    private final String cron;
    private final String globalCheckClass;
    /**
     * Child Table
     */
    private final TableConfig parentTC;
    private final String joinColumn;
    private final String parentColumn;
    private final String locateRTableKeySql;
    private final TableConfig directRouteTC;

    public TableConfig(String name, int sqlMaxLimit,
                       TableTypeEnum tableType, String shardingNode, RuleConfig rule, boolean sqlRequiredSharding, String incrementColumn,
                       String cron, String globalCheckClass, boolean globalCheck) {
        this(name, sqlMaxLimit, tableType, shardingNode, rule, sqlRequiredSharding,
                null, null, null, incrementColumn, cron, globalCheckClass, globalCheck);
    }

    public TableConfig(String name, int sqlMaxLimit,
                       TableTypeEnum tableType, String shardingNode, RuleConfig rule, boolean sqlRequiredSharding, TableConfig parentTC,
                       String joinColumn, String parentColumn, String incrementColumn, String cron, String globalCheckClass, boolean globalCheck) {
        if (name == null) {
            throw new IllegalArgumentException("table name is null");
        } else if (shardingNode == null) {
            throw new IllegalArgumentException("dataNode name is null");
        }
        this.incrementColumn = incrementColumn;
        this.maxLimit = sqlMaxLimit;
        this.tableType = tableType;
        this.cron = cron;
        this.globalCheckClass = globalCheckClass;
        this.globalCheck = globalCheck;
        if (sqlRequiredSharding && rule == null) {
            throw new IllegalArgumentException("sqlRequiredSharding but rule is null");
        }
        this.name = name;
        String[] theShardingNodes = SplitUtil.split(shardingNode, ',', '$', '-');
        if (theShardingNodes.length <= 0) {
            throw new IllegalArgumentException("invalid table dataNodes: " + shardingNode + " for table " + name);
        }
        if (tableType != TableTypeEnum.TYPE_GLOBAL_TABLE && parentTC == null && theShardingNodes.length > 1 && rule == null) {
            throw new IllegalArgumentException("invalid table dataNodes: " + shardingNode + " for table " + name);
        }
        isNoSharding = theShardingNodes.length == 1;
        shardingNodes = new ArrayList<>(theShardingNodes.length);
        Collections.addAll(shardingNodes, theShardingNodes);
        this.rule = rule;
        this.partitionColumn = (rule == null) ? null : rule.getColumn();
        this.sqlRequiredSharding = sqlRequiredSharding;
        this.parentTC = parentTC;
        if (parentTC != null) {
            this.joinColumn = joinColumn;
            this.parentColumn = parentColumn;
            if (parentTC.getParentTC() == null) {
                if (parentTC.partitionColumn.equals(parentColumn)) {
                    // secondLevel ,parentColumn==parent.partitionColumn
                    directRouteTC = parentTC;
                    locateRTableKeySql = null;
                } else {
                    directRouteTC = null;
                    locateRTableKeySql = genLocateRootParentSQL();
                }
            } else if (parentTC.getDirectRouteTC() != null) {
                /*
                 * grandTable partitionColumn =col1
                 * fatherTable joinkey =col2,parentkey = col1....so directRouteTC = grandTable
                 * thisTable joinkey = col3 ,parentkey = col2...so directRouteTC = grandTable
                 */
                if (parentTC.joinColumn.equals(parentColumn)) {
                    directRouteTC = parentTC.getDirectRouteTC();
                    locateRTableKeySql = null;
                } else {
                    directRouteTC = null;
                    locateRTableKeySql = genLocateRootParentSQL();
                }
            } else {
                directRouteTC = null;
                locateRTableKeySql = genLocateRootParentSQL();
            }
        } else {
            this.joinColumn = null;
            this.parentColumn = null;
            locateRTableKeySql = null;
            directRouteTC = this;
        }
    }

    public TableConfig(String name, int sqlMaxLimit,
                       TableTypeEnum tableType, ArrayList<String> shardingNodes, RuleConfig rule, boolean sqlRequiredSharding, TableConfig parentTC,
                       String joinColumn, String parentColumn, String incrementColumn, String cron, String globalCheckClass, boolean globalCheck) {
        this.maxLimit = sqlMaxLimit;
        this.tableType = tableType;
        this.name = name;
        this.cron = cron;
        this.globalCheckClass = globalCheckClass;
        this.globalCheck = globalCheck;
        this.shardingNodes = shardingNodes;
        this.rule = rule;
        this.partitionColumn = (rule == null) ? null : rule.getColumn();
        this.incrementColumn = incrementColumn;
        this.sqlRequiredSharding = sqlRequiredSharding;
        this.parentTC = parentTC;
        this.joinColumn = joinColumn;
        this.parentColumn = parentColumn;
        isNoSharding = shardingNodes.size() == 1;
        if (parentTC != null) {
            if (parentTC.getParentTC() == null) {
                if (parentColumn.equalsIgnoreCase(parentTC.partitionColumn)) {
                    // secondLevel ,parentColumn==parent.partitionColumn
                    directRouteTC = parentTC;
                    locateRTableKeySql = null;
                } else {
                    directRouteTC = null;
                    locateRTableKeySql = genLocateRootParentSQL();
                }
            } else if (parentTC.getDirectRouteTC() != null) {
                if (parentColumn.equals(parentTC.joinColumn)) {
                    directRouteTC = parentTC.getDirectRouteTC();
                    locateRTableKeySql = null;
                } else {
                    directRouteTC = null;
                    locateRTableKeySql = genLocateRootParentSQL();
                }
            } else {
                directRouteTC = null;
                locateRTableKeySql = genLocateRootParentSQL();
            }
        } else {
            locateRTableKeySql = null;
            directRouteTC = this;
        }
    }


    TableConfig lowerCaseCopy(TableConfig parent) {
        return new TableConfig(this.name.toLowerCase(), this.maxLimit,
                this.tableType, this.shardingNodes, this.rule, this.sqlRequiredSharding, parent, this.joinColumn, this.parentColumn, this.incrementColumn,
                this.cron, this.globalCheckClass, this.globalCheck);

    }

    public String getIncrementColumn() {
        return incrementColumn;
    }

    public boolean isAutoIncrement() {
        return incrementColumn != null;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public TableConfig getDirectRouteTC() {
        return directRouteTC;
    }

    public String getLocateRTableKeySql() {
        return locateRTableKeySql;
    }

    public boolean isGlobalTable() {
        return this.tableType == TableTypeEnum.TYPE_GLOBAL_TABLE;
    }

    private String genLocateRootParentSQL() {
        TableConfig tb = this;
        StringBuilder tableSb = new StringBuilder();
        StringBuilder condition = new StringBuilder();
        TableConfig prevTC = tb;
        int level = 0;
        String latestCond = null;
        while (tb.parentTC != null) {
            tableSb.append(tb.parentTC.name).append(',');
            if (level == 0) {
                latestCond = " " + tb.parentTC.getName() + '.' + tb.parentColumn + "=";
            } else {
                String relation = tb.parentTC.getName() + '.' + tb.parentColumn + '=' + tb.name + '.' + tb.joinColumn;
                condition.append(relation).append(" AND ");
            }
            level++;
            prevTC = tb;
            tb = tb.parentTC;
        }
        return "SELECT " +
                prevTC.parentTC.name +
                '.' +
                prevTC.parentColumn +
                " FROM " +
                tableSb.substring(0, tableSb.length() - 1) +
                " WHERE " +
                ((level < 2) ? latestCond : condition.toString() + latestCond);

    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public TableTypeEnum getTableType() {
        return tableType;
    }

    /**
     * get root parent
     */
    public TableConfig getRootParent() {
        if (parentTC == null) {
            return null;
        }
        TableConfig preParent = parentTC;
        TableConfig parent = preParent.getParentTC();

        while (parent != null) {
            preParent = parent;
            parent = parent.getParentTC();
        }
        return preParent;
    }

    public TableConfig getParentTC() {
        return parentTC;
    }

    public String getJoinColumn() {
        return joinColumn;
    }

    public String getParentColumn() {
        return parentColumn;
    }

    public String getName() {
        return name;
    }

    public ArrayList<String> getShardingNodes() {
        return shardingNodes;
    }

    public String getRandomShardingNode() {
        return RouterUtil.getRandomShardingNode(shardingNodes);
    }

    public boolean isSQLRequiredSharding() {
        return sqlRequiredSharding;
    }

    public RuleConfig getRule() {
        return rule;
    }


    public boolean isNoSharding() {
        return isNoSharding;
    }

    public boolean isGlobalCheck() {
        return globalCheck;
    }

    public String getCron() {
        return cron;
    }

    public String getGlobalCheckClass() {
        return globalCheckClass;
    }
}
