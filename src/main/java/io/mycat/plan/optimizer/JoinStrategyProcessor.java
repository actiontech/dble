package io.mycat.plan.optimizer;

import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.util.PlanUtil;

public final class JoinStrategyProcessor {
    private JoinStrategyProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        if (PlanUtil.isGlobalOrER(qtn))
            return qtn;
        if (qtn.type() == PlanNodeType.JOIN) {
            JoinNode jn = (JoinNode) qtn;
            if (jn.getLeftNode().type() == PlanNodeType.TABLE && jn.getRightNode().type() == PlanNodeType.TABLE) {
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
