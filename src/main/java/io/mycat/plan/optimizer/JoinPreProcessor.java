package io.mycat.plan.optimizer;

import io.mycat.plan.PlanNode;
import io.mycat.plan.node.JoinNode;

public final class JoinPreProcessor {
    private JoinPreProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        qtn = findAndChangeRightJoinToLeftJoin(qtn);
        return qtn;
    }

    /**
     * 会遍历所有节点将right join的左右节点进行调换，转换成left join.
     * <p>
     * <pre>
     * 比如 A right join B on A.id = B.id
     * 转化为 B left join B on A.id = B.id
     * </pre>
     */
    private static PlanNode findAndChangeRightJoinToLeftJoin(PlanNode qtn) {
        for (PlanNode child : qtn.getChildren()) {
            findAndChangeRightJoinToLeftJoin((PlanNode) child);
        }

        if (qtn instanceof JoinNode && ((JoinNode) qtn).isRightOuterJoin()) {
            JoinNode jn = (JoinNode) qtn;
            jn.exchangeLeftAndRight();
        }

        return qtn;
    }

}
