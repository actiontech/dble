/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.entity.sharding.schema;

import com.actiontech.dble.cluster.zkprocess.entity.Named;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "childTable")
public class ChildTable implements Named {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String joinColumn;
    @XmlAttribute(required = true)
    protected String parentColumn;
    @XmlAttribute
    protected String incrementColumn;

    @XmlAttribute
    protected Integer sqlMaxLimit;

    protected List<ChildTable> childTable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJoinColumn() {
        return joinColumn;
    }

    public void setJoinColumn(String joinColumn) {
        this.joinColumn = joinColumn;
    }

    public String getParentColumn() {
        return parentColumn;
    }

    public void setParentColumn(String parentColumn) {
        this.parentColumn = parentColumn;
    }

    public List<ChildTable> getChildTable() {
        if (this.childTable == null) {
            childTable = new ArrayList<>();
        }
        return childTable;
    }

    public void setChildTable(List<ChildTable> childTable) {
        this.childTable = childTable;
    }

    public String getIncrementColumn() {
        return incrementColumn;
    }

    public void setIncrementColumn(String incrementColumn) {
        this.incrementColumn = incrementColumn;
    }

    public Integer getSqlMaxLimit() {
        return sqlMaxLimit;
    }

    public void setSqlMaxLimit(Integer sqlMaxLimit) {
        this.sqlMaxLimit = sqlMaxLimit;
    }


    @Override
    public String toString() {
        return "ChildTable [name=" +
                name +
                ", joinColumn=" +
                joinColumn +
                ", parentColumn=" +
                parentColumn +
                ", incrementColumn=" +
                incrementColumn +
                ", sqlMaxLimit=" +
                sqlMaxLimit +
                ", childTable=" +
                childTable +
                "]";
    }

}
