/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model.rule;

/**
 * @author mycat
 */
public interface RuleAlgorithm {

    /**
     * init
     *
     * @param
     */
    void init();

    /**
     * return sharding nodes's id
     * columnValue is column's value
     *
     * @return never null
     */
    Integer calculate(String columnValue);

    Integer[] calculateRange(String beginValue, String endValue);
}
