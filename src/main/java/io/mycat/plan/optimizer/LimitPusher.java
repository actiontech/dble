package io.mycat.plan.optimizer;

import io.mycat.plan.PlanNode;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.MergeNode;
import io.mycat.plan.node.QueryNode;

public class LimitPusher {

    public static PlanNode optimize(PlanNode qtn) {
        qtn = findChild(qtn);
        return qtn;
    }

    private static PlanNode findChild(PlanNode qtn) {
        if (qtn instanceof MergeNode) {
            // limit优化
            // union直接把limit下发到各个子节点
            // union all下发各个子节点并且加上distinct
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
