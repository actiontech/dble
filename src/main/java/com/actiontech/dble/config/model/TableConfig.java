/*
* Copyright (C) 2016-2019 ActionTech.
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
    private final String primaryKey;
    private final boolean autoIncrement;
    private final String incrementColumn;
    private final boolean needAddLimit;
    private final TableTypeEnum tableType;
    private final ArrayList<String> dataNodes;
    private final RuleConfig rule;
    private final String partitionColumn;
    private final boolean ruleRequired;
    private final boolean partionKeyIsPrimaryKey;
    private final boolean isNoSharding;
    /**
     * Child Table
     */
    private final TableConfig parentTC;
    private final String joinKey;
    private final String parentKey;
    private final String locateRTableKeySql;
    private final TableConfig directRouteTC;

    public TableConfig(String name, String primaryKey, boolean autoIncrement, boolean needAddLimit,
                       TableTypeEnum tableType, String dataNode, RuleConfig rule, boolean ruleRequired, String incrementColumn) {
        this(name, primaryKey, autoIncrement, needAddLimit, tableType, dataNode, rule, ruleRequired, null, null, null, incrementColumn);
    }

    public TableConfig(String name, String primaryKey, boolean autoIncrement, boolean needAddLimit,
                       TableTypeEnum tableType, String dataNode, RuleConfig rule, boolean ruleRequired, TableConfig parentTC,
                       String joinKey, String parentKey, String incrementColumn) {
        if (name == null) {
            throw new IllegalArgumentException("table name is null");
        } else if (dataNode == null) {
            throw new IllegalArgumentException("dataNode name is null");
        }
        this.incrementColumn = incrementColumn;
        this.primaryKey = primaryKey;
        this.autoIncrement = autoIncrement;
        this.needAddLimit = needAddLimit;
        this.tableType = tableType;
        if (ruleRequired && rule == null) {
            throw new IllegalArgumentException("ruleRequired but rule is null");
        }
        this.name = name;
        String[] theDataNodes = SplitUtil.split(dataNode, ',', '$', '-');
        if (theDataNodes.length <= 0) {
            throw new IllegalArgumentException("invalid table dataNodes: " + dataNode + " for table " + name);
        }
        if (tableType != TableTypeEnum.TYPE_GLOBAL_TABLE && parentTC == null && theDataNodes.length > 1 && rule == null) {
            throw new IllegalArgumentException("invalid table dataNodes: " + dataNode + " for table " + name);
        }
        isNoSharding = theDataNodes.length == 1;
        dataNodes = new ArrayList<>(theDataNodes.length);
        Collections.addAll(dataNodes, theDataNodes);
        this.rule = rule;
        this.partitionColumn = (rule == null) ? null : rule.getColumn();
        partionKeyIsPrimaryKey = (partitionColumn == null) ? primaryKey == null : partitionColumn.equals(primaryKey);
        this.ruleRequired = ruleRequired;
        this.parentTC = parentTC;
        if (parentTC != null) {
            this.joinKey = joinKey;
            this.parentKey = parentKey;
            if (parentTC.getParentTC() == null) {
                if (parentKey.equals(parentTC.partitionColumn)) {
                    // secondLevel ,parentKey==parent.partitionColumn
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
                if (parentKey.equals(parentTC.joinKey)) {
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
            this.joinKey = null;
            this.parentKey = null;
            locateRTableKeySql = null;
            directRouteTC = this;
        }
    }


    public TableConfig(String name, String primaryKey, boolean autoIncrement, boolean needAddLimit,
                       TableTypeEnum tableType, ArrayList<String> dataNode, RuleConfig rule, boolean ruleRequired, TableConfig parentTC,
                       String joinKey, String parentKey, String incrementColumn) {
        this.primaryKey = primaryKey;
        this.autoIncrement = autoIncrement;
        this.needAddLimit = needAddLimit;
        this.tableType = tableType;
        this.name = name;
        this.dataNodes = dataNode;
        this.rule = rule;
        this.partitionColumn = (rule == null) ? null : rule.getColumn();
        this.incrementColumn = incrementColumn;
        partionKeyIsPrimaryKey = (partitionColumn == null) ? primaryKey == null : partitionColumn.equals(primaryKey);
        this.ruleRequired = ruleRequired;
        this.parentTC = parentTC;
        this.joinKey = joinKey;
        this.parentKey = parentKey;
        isNoSharding = dataNodes.size() == 1;
        if (parentTC != null) {
            if (parentTC.getParentTC() == null) {
                if (parentKey.equalsIgnoreCase(parentTC.partitionColumn)) {
                    // secondLevel ,parentKey==parent.partitionColumn
                    directRouteTC = parentTC;
                    locateRTableKeySql = null;
                } else {
                    directRouteTC = null;
                    locateRTableKeySql = genLocateRootParentSQL();
                }
            } else if (parentTC.getDirectRouteTC() != null) {
                if (parentKey.equals(parentTC.joinKey)) {
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
        return new TableConfig(this.name.toLowerCase(), this.primaryKey, this.autoIncrement, this.needAddLimit,
                this.tableType, this.dataNodes, this.rule, this.ruleRequired, parent, this.joinKey, this.parentKey, this.incrementColumn);

    }

    public String getTrueIncrementColumn() {
        return incrementColumn != null ? incrementColumn : primaryKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public boolean isNeedAddLimit() {
        return needAddLimit;
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
        TableConfig prevTC = null;
        int level = 0;
        String latestCond = null;
        while (tb.parentTC != null) {
            tableSb.append(tb.parentTC.name).append(',');
            String relation = null;
            if (level == 0) {
                latestCond = " " + tb.parentTC.getName() + '.' + tb.parentKey + "=";
            } else {
                relation = tb.parentTC.getName() + '.' + tb.parentKey + '=' + tb.name + '.' + tb.joinKey;
                condition.append(relation).append(" AND ");
            }
            level++;
            prevTC = tb;
            tb = tb.parentTC;
        }
        String sql = "SELECT " +
                prevTC.parentTC.name +
                '.' +
                prevTC.parentKey +
                " FROM " +
                tableSb.substring(0, tableSb.length() - 1) +
                " WHERE " +
                ((level < 2) ? latestCond : condition.toString() + latestCond);
        return sql;

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

    public String getJoinKey() {
        return joinKey;
    }

    public String getParentKey() {
        return parentKey;
    }

    public String getName() {
        return name;
    }

    public ArrayList<String> getDataNodes() {
        return dataNodes;
    }

    public String getRandomDataNode() {
        return RouterUtil.getRandomDataNode(dataNodes);
    }

    public boolean isRuleRequired() {
        return ruleRequired;
    }

    public RuleConfig getRule() {
        return rule;
    }

    public boolean primaryKeyIsPartionKey() {
        return partionKeyIsPrimaryKey;
    }


    public boolean isNoSharding() {
        return isNoSharding;
    }

}
