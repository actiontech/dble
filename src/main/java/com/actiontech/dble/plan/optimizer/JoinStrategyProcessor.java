/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

public final class JoinStrategyProcessor {
    public static final String NEED_REPLACE = "{NEED_TO_REPLACE}";

    private JoinStrategyProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-nest-loop");
        try {

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
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }
}
