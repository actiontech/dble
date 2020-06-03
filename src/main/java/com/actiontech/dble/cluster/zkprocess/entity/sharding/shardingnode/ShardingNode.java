/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.sharding.shardingnode;

import com.actiontech.dble.cluster.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "shardingNode")
public class ShardingNode implements Named {

    @XmlAttribute(required = true)
    private String name;

    @XmlAttribute(required = true)
    private String dbGroup;

    @XmlAttribute(required = true)
    private String database;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbGroup() {
        return dbGroup;
    }

    public void setDbGroup(String dbGroup) {
        this.dbGroup = dbGroup;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public String toString() {
        String builder = "ShardingNode [name=" +
                name +
                ", dbGroup=" +
                dbGroup +
                ", database=" +
                database +
                "]";
        return builder;
    }

}
