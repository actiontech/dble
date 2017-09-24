/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model.rule;

import java.io.Serializable;

/**
 * @author mycat
 */
public class TableRuleConfig implements Serializable {
    //maybe become a list in feature
    private final RuleConfig rule;

    public TableRuleConfig(String name, RuleConfig rule) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }
        if (rule == null) {
            throw new IllegalArgumentException("no rule is found");
        }
        this.rule = rule;
    }

    /**
     * @return unmodifiable
     */
    public RuleConfig getRule() {
        return rule;
    }

}
