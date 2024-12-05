/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.schema;

import com.oceanbase.obsharding_d.cluster.zkprocess.entity.Named;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "schema")
public class Schema implements Named {

    /**
     * sharding name
     */
    @XmlAttribute(required = true)
    protected String name;


    @XmlAttribute
    protected Integer sqlMaxLimit;

    @XmlAttribute
    protected boolean logicalCreateADrop = true;

    @XmlAttribute
    protected String shardingNode;

    @XmlAttribute
    protected String function;

    @XmlElementRefs({
            @XmlElementRef(name = "SingleTable", type = SingleTable.class),
            @XmlElementRef(name = "ShardingTable", type = ShardingTable.class),
            @XmlElementRef(name = "GlobalTable", type = GlobalTable.class)
    })
    protected List<Object> table;

    public List<Object> getTable() {
        return table;
    }

    public void setTable(List<Object> table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSqlMaxLimit() {
        return sqlMaxLimit;
    }

    public void setSqlMaxLimit(Integer sqlMaxLimit) {
        this.sqlMaxLimit = sqlMaxLimit;
    }

    public String getShardingNode() {
        return shardingNode;
    }

    public void setShardingNode(String shardingNode) {
        this.shardingNode = shardingNode;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public boolean isLogicalCreateADrop() {
        return logicalCreateADrop;
    }

    @Override
    public String toString() {
        return "Schema [name=" +
                name +
                ", sqlMaxLimit=" +
                sqlMaxLimit +
                ", shardingNode=" +
                shardingNode +
                ", function=" +
                function +
                ", table=" +
                table +
                "]";
    }

}
