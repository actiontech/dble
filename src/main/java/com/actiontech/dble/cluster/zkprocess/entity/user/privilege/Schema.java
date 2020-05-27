/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.user.privilege;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huqing.yan on 2017/6/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "schema", namespace = "privileges")
public class Schema {
    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String dml;
    protected List<Table> table;

    public List<Table> getTable() {
        if (this.table == null) {
            table = new ArrayList<>();
        }
        return table;
    }

    public void setTable(List<Table> table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDml() {
        return dml;
    }

    public void setDml(String dml) {
        this.dml = dml;
    }

    @Override
    public String toString() {
        return "schema{" + "name='" + name + '\'' + ", dml='" + dml + '\'' + ", tables=" + table + '}';
    }
}
