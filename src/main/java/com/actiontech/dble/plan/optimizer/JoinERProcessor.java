/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

public final class JoinERProcessor {
    private JoinERProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-er-relation");
        try {
            if (qtn instanceof JoinNode) {
                qtn = new ERJoinChooser((JoinNode) qtn).optimize();
            } else {
                for (int i = 0; i < qtn.getChildren().size(); i++) {
                    PlanNode sub = qtn.getChildren().get(i);
                    qtn.getChildren().set(i, optimize(sub));
                }
            }
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }
}
