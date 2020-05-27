/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.sharding.schema;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "shardingTable")
@XmlRootElement
public class ShardingTable extends Table {
    @XmlAttribute(required = true)
    protected String function;
    @XmlAttribute(required = true)
    protected String shardingColumn;
    @XmlAttribute
    protected Boolean sqlRequiredSharding;
    @XmlAttribute
    protected String incrementColumn;

    protected List<ChildTable> childTable;

    public List<ChildTable> getChildTable() {
        if (this.childTable == null) {
            childTable = new ArrayList<>();
        }
        return childTable;
    }

    public void setChildTable(List<ChildTable> childTable) {
        this.childTable = childTable;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getShardingColumn() {
        return shardingColumn;
    }

    public void setShardingColumn(String shardingColumn) {
        this.shardingColumn = shardingColumn;
    }

    public Boolean getSqlRequiredSharding() {
        return sqlRequiredSharding;
    }

    public void setSqlRequiredSharding(Boolean sqlRequiredSharding) {
        this.sqlRequiredSharding = sqlRequiredSharding;
    }

    public String getIncrementColumn() {
        return incrementColumn;
    }

    public void setIncrementColumn(String incrementColumn) {
        this.incrementColumn = incrementColumn;
    }

    @Override
    public String toString() {
        return "ShardingTable [" + super.toString() +
                ", function=" +
                function +
                ", shardingColumn=" +
                shardingColumn +
                ", incrementColumn=" +
                incrementColumn +
                ", sqlRequiredSharding=" +
                sqlRequiredSharding +
                ", childTable=" +
                childTable +
                "]";
    }

}
