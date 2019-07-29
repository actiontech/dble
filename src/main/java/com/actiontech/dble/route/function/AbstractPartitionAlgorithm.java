/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * AbstractPartitionAlgorithm
 *
 * @author lxy
 */
public abstract class AbstractPartitionAlgorithm implements RuleAlgorithm, Serializable {
    protected String alias;
    protected String name;

    protected Map<String, String> propertiesMap = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    private static final long serialVersionUID = -532594213256935577L;

    @Override
    public void init() {
    }

    /**
     * return the index of node
     * retrun an empty array means router to all node
     * return null if no node matches
     */
    @Override
    public abstract Integer[] calculateRange(String beginValue, String endValue);

    /**
     * valid the consistency between table's node size and rule's node size
     *
     * @param dnSize
     * @return -1 if table datanode size < rule function partition size
     * 0 if table datanode size == rule function partition size
     * 1 if table datanode size > rule function partition size
     */
    public final int suitableFor(int dnSize) {
        int nPartition = getPartitionNum();
        if (nPartition > 0) {
            if (dnSize < nPartition) {
                return -1;
            } else if (dnSize > nPartition) {
                return 1;
            }
        }
        return 0;
    }

    /**
     * getPartitionNum, return -1 means no limit
     *
     * @return
     */
    public int getPartitionNum() {
        return -1; // no limit
    }

    @Override
    public Map<String, String> getAllProperties() {
        return propertiesMap;
    }

}
