/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model.rule;

import java.util.Map;

/**
 * @author mycat
 */
public interface RuleAlgorithm {

    void init();

    /**
     * return sharding nodes's index
     * @return never null
     */
    Integer calculate(String columnValue);

    /**
     * return the index of node
     * return an empty array means router to all node
     * return null if no node matches
     * only support long and Date
     */
    Integer[] calculateRange(String beginValue, String endValue);

    Map<String, String> getAllProperties();

    void selfCheck();
}
