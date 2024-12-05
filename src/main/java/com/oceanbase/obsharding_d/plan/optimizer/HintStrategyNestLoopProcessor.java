/*
 * Copyright (C) 2016-2021 oceanbase.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public final class HintStrategyNestLoopProcessor {

    private HintStrategyNestLoopProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn, HintPlanInfo hintPlanInfo) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-hint-for-nest-loop");
        try {
            if (qtn instanceof JoinNode) {
                JoinNestLoopChooser joinNestLoopChooser = new JoinNestLoopChooser((JoinNode) qtn, hintPlanInfo);
                joinNestLoopChooser.tryNestLoop();
            } else {
                List<PlanNode> children = qtn.getChildren();
                for (PlanNode child : children) {
                    optimize(child, hintPlanInfo);
                }
            }
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }


}
