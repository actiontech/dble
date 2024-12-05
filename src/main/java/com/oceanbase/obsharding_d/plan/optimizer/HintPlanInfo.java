/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.server.parser.HintPlanParse;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private LinkedHashMap<String, HintPlanParse.Type> hintPlanNodeMap = Maps.newLinkedHashMap();

    public HintPlanInfo() {
    }

    public void setRelationMap(HintPlanParse parse) {
        this.dependMap = parse.getDependMap();
        this.erMap = parse.getErMap();
        this.hintPlanNodeMap = parse.getHintPlanNodeMap();
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

    public LinkedHashMap<String, HintPlanParse.Type> getHintPlanNodeMap() {
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
