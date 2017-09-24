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
 * <table name="travelrecord" dataNode="dn1,dn2,dn3" rule="auto-sharding-long" />
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "table")
public class Table implements Named {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected String nameSuffix;
    @XmlAttribute(required = true)
    protected String dataNode;
    @XmlAttribute
    protected String rule;
    @XmlAttribute
    protected Boolean ruleRequired;
    @XmlAttribute
    protected String primaryKey;
    @XmlAttribute
    protected Boolean autoIncrement;
    @XmlAttribute
    protected Boolean needAddLimit;
    @XmlAttribute
    protected String type;

    protected List<ChildTable> childTable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
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

    public String getNameSuffix() {
        return nameSuffix;
    }

    public void setNameSuffix(String nameSuffix) {
        this.nameSuffix = nameSuffix;
    }

    public Boolean isRuleRequired() {
        return ruleRequired;
    }

    public void setRuleRequired(Boolean ruleRequired) {
        this.ruleRequired = ruleRequired;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(Boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public Boolean isNeedAddLimit() {
        return needAddLimit;
    }

    public void setNeedAddLimit(Boolean needAddLimit) {
        this.needAddLimit = needAddLimit;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        String builder = "Table [name=" +
                name +
                ", nameSuffix=" +
                nameSuffix +
                ", dataNode=" +
                dataNode +
                ", rule=" +
                rule +
                ", ruleRequired=" +
                ruleRequired +
                ", primaryKey=" +
                primaryKey +
                ", autoIncrement=" +
                autoIncrement +
                ", needAddLimit=" +
                needAddLimit +
                ", type=" +
                type +
                ", childTable=" +
                childTable +
                "]";
        return builder;
    }

}
