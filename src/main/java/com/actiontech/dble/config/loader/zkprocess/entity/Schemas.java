/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost.DataHost;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.datanode.DataNode;
import com.actiontech.dble.config.loader.zkprocess.entity.schema.schema.Schema;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = Versions.DOMAIN, name = "schema")
public class Schemas {
    private List<Schema> schema;

    private List<DataNode> dataNode;

    private List<DataHost> dataHost;

    public List<Schema> getSchema() {
        if (this.schema == null) {
            schema = new ArrayList<>();
        }
        return schema;
    }

    public void setSchema(List<Schema> schema) {
        this.schema = schema;
    }

    public List<DataNode> getDataNode() {
        if (this.dataNode == null) {
            dataNode = new ArrayList<>();
        }
        return dataNode;
    }

    public void setDataNode(List<DataNode> dataNode) {
        this.dataNode = dataNode;
    }

    public List<DataHost> getDataHost() {
        if (this.dataHost == null) {
            dataHost = new ArrayList<>();
        }
        return dataHost;
    }

    public void setDataHost(List<DataHost> dataHost) {
        this.dataHost = dataHost;
    }

    @Override
    public String toString() {
        String builder = "Schemas [schema=" +
                schema +
                ", dataNode=" +
                dataNode +
                ", dataHost=" +
                dataHost +
                "]";
        return builder;
    }

}
