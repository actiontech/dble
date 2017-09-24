/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.rule.tablerule;

import com.actiontech.dble.config.loader.zkprocess.entity.Named;

import javax.xml.bind.annotation.*;

/**
 * * <tableRule name="rule1">
 * * *<rule>
 * * * *<columns>id</columns>
 * * * *<algorithm>func1</algorithm>
 * * </rule>
 * </tableRule>
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/18
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "tableRule")
public class TableRule implements Named {

    @XmlElement(required = true, name = "rule")
    protected Rule rule;

    @XmlAttribute(required = true)
    protected String name;

    public Rule getRule() {
        return rule;
    }

    public TableRule setRule(Rule tableRule) {
        this.rule = tableRule;
        return this;
    }

    public String getName() {
        return name;
    }

    public TableRule setName(String tableName) {
        this.name = tableName;
        return this;
    }

    @Override
    public String toString() {
        String builder = "TableRule [rule=" +
                rule +
                ", name=" +
                name +
                "]";
        return builder;
    }

}
