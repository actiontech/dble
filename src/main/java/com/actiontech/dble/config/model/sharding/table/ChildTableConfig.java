/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.sharding.table;

import java.util.List;

public class ChildTableConfig extends BaseTableConfig {
    private final BaseTableConfig parentTC;
    private final String joinColumn;
    private final String parentColumn;
    private final String incrementColumn;
    private final String locateRTableKeySql;
    private final ShardingTableConfig directRouteTC;

    public ChildTableConfig(String name, int maxLimit, List<String> shardingNodes, BaseTableConfig parentTC, String joinColumn, String parentColumn, String incrementColumn) {
        super(name, maxLimit, shardingNodes);
        this.parentTC = parentTC;
        this.joinColumn = joinColumn;
        this.parentColumn = parentColumn;
        this.incrementColumn = incrementColumn;
        this.directRouteTC = findDirectRouteTC();
        if (directRouteTC == null) {
            locateRTableKeySql = genLocateRootParentSQL();
        } else {
            locateRTableKeySql = null;
        }
    }

    @Override
    public BaseTableConfig lowerCaseCopy(BaseTableConfig parent) {
        return new ChildTableConfig(this.name.toLowerCase(), this.maxLimit, this.shardingNodes,
                parent, this.joinColumn, this.parentColumn, this.incrementColumn);
    }


    private ShardingTableConfig findDirectRouteTC() {
        if (parentTC instanceof ShardingTableConfig) {
            ShardingTableConfig parent = (ShardingTableConfig) parentTC;
            if (parent.getShardingColumn().equals(parentColumn)) {
                // secondLevel ,parentColumn==parent.partitionColumn
                return parent;
            } else {
                return null;
            }
        } else {
            ChildTableConfig parent = (ChildTableConfig) parentTC;
            if (parent.getDirectRouteTC() != null) {
                /*
                 * grandTable partitionColumn =col1
                 * fatherTable joinkey =col2,parentkey = col1....so directRouteTC = grandTable
                 * thisTable joinkey = col3 ,parentkey = col2...so directRouteTC = grandTable
                 */
                if (parent.joinColumn.equals(parentColumn)) {
                    return parent.getDirectRouteTC();
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    private String genLocateRootParentSQL() {
        BaseTableConfig tb = this;
        StringBuilder tableSb = new StringBuilder();
        StringBuilder condition = new StringBuilder();
        BaseTableConfig prevTC = tb;
        int level = 0;
        String latestCond = null;
        while (tb instanceof ChildTableConfig) {
            ChildTableConfig thisTable = (ChildTableConfig) tb;
            tableSb.append(thisTable.parentTC.name).append(',');
            if (level == 0) {
                latestCond = " " + thisTable.parentTC.getName() + '.' + thisTable.parentColumn + "=";
            } else {
                String relation = thisTable.parentTC.getName() + '.' + thisTable.parentColumn + '=' + tb.name + '.' + thisTable.joinColumn;
                condition.append(relation).append(" AND ");
            }
            level++;
            prevTC = tb;
            tb = thisTable.parentTC;
        }
        ChildTableConfig firstGenChildTable = (ChildTableConfig) prevTC;
        return "SELECT " +
                firstGenChildTable.parentTC.name +
                '.' +
                firstGenChildTable.parentColumn +
                " FROM " +
                tableSb.substring(0, tableSb.length() - 1) +
                " WHERE " +
                ((level < 2) ? latestCond : condition.toString() + latestCond);

    }
    /**
     * get root parent
     */
    public ShardingTableConfig getRootParent() {
        BaseTableConfig preParent = parentTC;
        while (preParent instanceof ChildTableConfig) {
            preParent = ((ChildTableConfig) preParent).parentTC;
        }
        return (ShardingTableConfig) preParent;
    }
    public BaseTableConfig getParentTC() {
        return parentTC;
    }

    public String getJoinColumn() {
        return joinColumn;
    }

    public String getParentColumn() {
        return parentColumn;
    }

    public String getIncrementColumn() {
        return incrementColumn;
    }

    public String getLocateRTableKeySql() {
        return locateRTableKeySql;
    }

    public ShardingTableConfig getDirectRouteTC() {
        return directRouteTC;
    }

}
