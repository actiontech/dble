/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity;

import com.actiontech.dble.cluster.zkprocess.entity.sharding.function.Function;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Schema;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.shardingnode.ShardingNode;
import com.actiontech.dble.config.Versions;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = Versions.DOMAIN, name = "sharding")
public class Shardings {
    @XmlAttribute(required = false)
    protected String version;

    private List<Schema> schema;

    private List<ShardingNode> shardingNode;

    protected List<Function> function;

    public List<Schema> getSchema() {
        if (this.schema == null) {
            schema = new ArrayList<>();
        }
        return schema;
    }

    public void setSchema(List<Schema> schema) {
        this.schema = schema;
    }

    public List<ShardingNode> getShardingNode() {
        if (this.shardingNode == null) {
            shardingNode = new ArrayList<>();
        }
        return shardingNode;
    }

    public void setShardingNode(List<ShardingNode> shardingNode) {
        this.shardingNode = shardingNode;
    }


    public List<Function> getFunction() {
        if (this.function == null) {
            function = new ArrayList<>();
        }
        return function;
    }

    public void setFunction(List<Function> function) {
        this.function = function;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "sharding [schema=" +
                schema +
                ", shardingNode=" +
                shardingNode +
                ", function=" +
                function +
                "]";
    }

}
