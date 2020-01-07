/*
 * Copyright (C) 2016-2020 ActionTech.
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
 * <childTable name="order_items" joinKey="order_id" parentKey="id" />
 * ChildTable
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "childTable")
public class ChildTable implements Named {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String joinKey;
    @XmlAttribute(required = true)
    protected String parentKey;
    @XmlAttribute
    protected String cacheKey;
    @XmlAttribute
    protected String incrementColumn;

    @XmlAttribute
    protected Boolean needAddLimit;

    protected List<ChildTable> childTable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public String getParentKey() {
        return parentKey;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
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

    public Boolean getNeedAddLimit() {
        return needAddLimit;
    }

    public void setNeedAddLimit(Boolean needAddLimit) {
        this.needAddLimit = needAddLimit;
    }


    @Override
    public String toString() {
        String builder = "ChildTable [name=" +
                name +
                ", joinKey=" +
                joinKey +
                ", parentKey=" +
                parentKey +
                ", cacheKey=" +
                cacheKey +
                ", childTable=" +
                childTable +
                "]";
        return builder;
    }

}
