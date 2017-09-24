/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.schema.schema;

import com.actiontech.dble.config.loader.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * <schema name="TESTDB" checkSQLschema="false" sqlMaxLimit="100">
 * * <table name="travelrecord" dataNode="dn1,dn2,dn3" rule="auto-sharding-long" />
 * *
 * </schema>
 * <p>
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "schema")
public class Schema implements Named {

    /**
     * schema name
     */
    @XmlAttribute(required = true)
    protected String name;


    @XmlAttribute
    protected Integer sqlMaxLimit;

    @XmlAttribute
    protected String dataNode;

    protected List<Table> table;

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

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

    public List<Table> getTable() {
        if (this.table == null) {
            table = new ArrayList<>();
        }
        return table;
    }

    public void setTable(List<Table> table) {
        this.table = table;
    }

    @Override
    public String toString() {
        String builder = "Schema [name=" +
                name +
                ", sqlMaxLimit=" +
                sqlMaxLimit +
                ", dataNode=" +
                dataNode +
                ", table=" +
                table +
                "]";
        return builder;
    }

}
