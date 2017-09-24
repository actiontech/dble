/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.util.PlanUtil;

public final class JoinStrategyProcessor {
    private JoinStrategyProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        if (PlanUtil.isGlobalOrER(qtn))
            return qtn;
        if (qtn.type() == PlanNode.PlanNodeType.JOIN) {
            JoinNode jn = (JoinNode) qtn;
            if (jn.getLeftNode().type() == PlanNode.PlanNodeType.TABLE && jn.getRightNode().type() == PlanNode.PlanNodeType.TABLE) {
                JoinStrategyChooser chooser = new JoinStrategyChooser((JoinNode) qtn);
                chooser.tryNestLoop();
                return qtn;
            }
        }
        for (PlanNode child : qtn.getChildren())
            optimize(child);
        return qtn;
    }
}
