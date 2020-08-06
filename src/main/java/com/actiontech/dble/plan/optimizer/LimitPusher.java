/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;

public final class LimitPusher {
    private LimitPusher() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-limit");
        try {
            qtn = findChild(qtn);
            return qtn;
        } finally {
            TraceManager.log(ImmutableMap.of("plan-node", qtn), traceObject);
            TraceManager.finishSpan(traceObject);
        }
    }

    private static PlanNode findChild(PlanNode qtn) {
        if (qtn instanceof MergeNode) {
            // optimizer limit
            // union: push down limit to children
            // union all:push down limit to children and add distinct
            MergeNode node = (MergeNode) qtn;
            long limitFrom = node.getLimitFrom();
            long limitTo = node.getLimitTo();
            if (limitFrom != -1 && limitTo != -1) {
                for (PlanNode child : node.getChildren()) {
                    pushLimit(child, limitFrom, limitTo, node.isUnion());
                }
            }

        } else if ((qtn instanceof JoinNode) || (qtn instanceof QueryNode)) {
            for (PlanNode child : qtn.getChildren()) {
                findChild(child);
            }
        }
        return qtn;
    }

    private static void pushLimit(PlanNode node, long limitFrom, long limitTo, boolean isUnion) {
        if (isUnion) {
            node.setDistinct(true);
        }
        if (node.getLimitFrom() == -1 && node.getLimitTo() == -1) {
            node.setLimitFrom(0);
            node.setLimitTo(limitFrom + limitTo);
        }
    }

}
