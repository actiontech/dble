/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.config.model.rule;

import io.mycat.route.function.AbstractPartitionAlgorithm;

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
