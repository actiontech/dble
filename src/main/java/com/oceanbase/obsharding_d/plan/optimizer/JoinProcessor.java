/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

public final class JoinProcessor {
    private JoinProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn, @Nonnull HintPlanInfo hintPlanInfo) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-er-relation");
        try {
            if (qtn instanceof JoinNode) {
                qtn = new JoinChooser((JoinNode) qtn, hintPlanInfo).optimize();
            } else {
                for (int i = 0; i < qtn.getChildren().size(); i++) {
                    PlanNode sub = qtn.getChildren().get(i);
                    qtn.getChildren().set(i, optimize(sub, hintPlanInfo));
                }
            }
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }
}
