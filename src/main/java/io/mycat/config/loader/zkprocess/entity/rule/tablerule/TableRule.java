package io.mycat.config.loader.zkprocess.entity.rule.tablerule;

import io.mycat.config.loader.zkprocess.entity.Named;

import javax.xml.bind.annotation.*;

/**
 * * <tableRule name="rule1">
 * * *<rule>
 * * * *<columns>id</columns>
 * * * *<algorithm>func1</algorithm>
 * * </rule>
 * </tableRule>
 *
 *
 * author:liujun
 * Created:2016/9/18
 *
 *
 *
 *
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
