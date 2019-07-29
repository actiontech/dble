/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity;

import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.loader.zkprocess.entity.rule.function.Function;
import com.actiontech.dble.config.loader.zkprocess.entity.rule.tablerule.TableRule;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = Versions.DOMAIN, name = "rule")
public class Rules {

    @XmlAttribute(required = false)
    protected String version;

    protected List<TableRule> tableRule;

    protected List<Function> function;

    public List<TableRule> getTableRule() {
        if (this.tableRule == null) {
            tableRule = new ArrayList<>();
        }
        return tableRule;
    }

    public void setTableRule(List<TableRule> tableRule) {
        this.tableRule = tableRule;
    }

    public List<Function> getFunction() {
        if (this.function == null) {
            function = new ArrayList<>();
        }
        return function;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setFunction(List<Function> function) {
        this.function = function;
    }

    @Override
    public String toString() {
        String builder = "Rules [tableRule=" +
                tableRule +
                ", function=" +
                function +
                "]";
        return builder;
    }


}
