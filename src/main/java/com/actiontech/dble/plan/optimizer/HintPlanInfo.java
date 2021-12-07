/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.optimizer;

import java.util.LinkedList;

/**
 * @author collapsar
 */
public final class HintPlanInfo {

    private final LinkedList<HintPlanNode> hintNodes;
    private boolean left2inner = false;
    private boolean in2join = false;

    public HintPlanInfo(LinkedList<HintPlanNode> hintNodes) {
        this.hintNodes = hintNodes;
    }

    public LinkedList<HintPlanNode> getHintNodes() {
        return hintNodes;
    }

    public boolean isLeft2inner() {
        return left2inner;
    }

    public boolean isIn2join() {
        return in2join;
    }

    public void setLft2inner(boolean isLeft2inner) {
        this.left2inner = isLeft2inner;
    }

    public void setIn2join(boolean in2join) {
        this.in2join = in2join;
    }
}
