/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.server.parser.HintPlanParse;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Set;

/**
 * @author collapsar
 */
public final class HintPlanInfo {

    private boolean left2inner = false;
    private boolean right2inner = false;
    private boolean in2join = false;

    private HashMap<String, Set<HintPlanNode>> dependMap = Maps.newHashMap();
    private HashMap<String, Set<HintPlanNode>> erMap = Maps.newHashMap();
    private HashMap<String, HintPlanParse.Type> hintPlanNodeMap = Maps.newHashMap();

    public HintPlanInfo(HashMap<String, Set<HintPlanNode>> dependMap, HashMap<String, Set<HintPlanNode>> erMap, HashMap<String, HintPlanParse.Type> hintPlanNodeMap) {
        this.dependMap = dependMap;
        this.erMap = erMap;
        this.hintPlanNodeMap = hintPlanNodeMap;
    }

    public HintPlanInfo() {
    }

    public boolean isLeft2inner() {
        return left2inner;
    }

    public boolean isIn2join() {
        return in2join;
    }

    public void setLeft2inner(boolean isLeft2inner) {
        this.left2inner = isLeft2inner;
    }

    public void setIn2join(boolean in2join) {
        this.in2join = in2join;
    }

    public boolean isRight2inner() {
        return right2inner;
    }

    public void setRight2inner(boolean right2inner) {
        this.right2inner = right2inner;
    }

    /**
     * return true if no node exists or hint is not set.
     *
     * @return
     */
    public boolean isZeroNode() {
        return hintPlanNodeMap.isEmpty();
    }

    public HashMap<String, Set<HintPlanNode>> getDependMap() {
        return dependMap;
    }

    public HashMap<String, Set<HintPlanNode>> getErMap() {
        return erMap;
    }

    public HashMap<String, HintPlanParse.Type> getHintPlanNodeMap() {
        return hintPlanNodeMap;
    }

    public int nodeSize() {
        return hintPlanNodeMap.size();
    }

    public boolean erRelyOn() {
        long count = erMap.keySet().stream().filter(table -> dependMap.containsKey(table)).count();
        return count > 0;
    }

    @Override
    public String toString() {
        return "HintPlanInfo{" +
                "hintPlanNodeMap=" + hintPlanNodeMap.toString() +
                "erMap=" + erMap.toString() +
                "dependMap=" + dependMap.toString() +
                ", left2inner=" + left2inner +
                ", in2join=" + in2join +
                '}';
    }

}
