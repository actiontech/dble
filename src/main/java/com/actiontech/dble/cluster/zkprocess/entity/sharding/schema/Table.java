/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.sharding.schema;

import com.actiontech.dble.cluster.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

@XmlTransient
@XmlSeeAlso({SingleTable.class, ShardingTable.class, GlobalTable.class})
public abstract class Table implements Named {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String shardingNode;
    @XmlAttribute
    protected Integer sqlMaxLimit;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getShardingNode() {
        return shardingNode;
    }

    public void setShardingNode(String shardingNode) {
        this.shardingNode = shardingNode;
    }



    public Integer getSqlMaxLimit() {
        return sqlMaxLimit;
    }

    public void setSqlMaxLimit(Integer sqlMaxLimit) {
        this.sqlMaxLimit = sqlMaxLimit;
    }

    @Override
    public String toString() {
        return "name=" +
                name +
                ", shardingNode=" +
                shardingNode +
                ", sqlMaxLimit=" +
                sqlMaxLimit;
    }
}
