/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model.rule;

import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;

import java.io.Serializable;

/**
 * RuleConfig
 *
 * @author mycat
 */
public class RuleConfig implements Serializable {
    private final String column;
    private final String functionName;
    private AbstractPartitionAlgorithm ruleAlgorithm;

    public RuleConfig(String column, String functionName, AbstractPartitionAlgorithm ruleAlgorithm) {
        this.functionName = functionName;
        this.column = column;
        this.ruleAlgorithm = ruleAlgorithm;
    }


    public AbstractPartitionAlgorithm getRuleAlgorithm() {
        return ruleAlgorithm;
    }

    /**
     * @return unmodifiable, upper-case
     */
    public String getColumn() {
        return column;
    }

    public String getFunctionName() {
        return functionName;
    }


}
