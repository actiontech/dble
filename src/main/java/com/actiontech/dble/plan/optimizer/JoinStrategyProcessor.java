/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public final class JoinStrategyProcessor {
    public static final String NEED_REPLACE = "{NEED_TO_REPLACE}";

    private JoinStrategyProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn, boolean always) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-nest-loop");
        try {
            if (PlanUtil.isGlobalOrER(qtn))
                return qtn;
            if (qtn instanceof JoinNode) {
                JoinStrategyChooser chooser = new JoinStrategyChooser((JoinNode) qtn);
                chooser.tryNestLoop(always);
                //todo log
                return qtn;
            }
            List<PlanNode> children = qtn.getChildren();
            for (PlanNode child : children) {
                optimize(child, always);
            }
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }
}
