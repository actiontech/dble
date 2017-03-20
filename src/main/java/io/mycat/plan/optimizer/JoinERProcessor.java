package io.mycat.plan.optimizer;

import io.mycat.plan.PlanNode;
import io.mycat.plan.node.JoinNode;

public class JoinERProcessor {
	public static PlanNode optimize(PlanNode qtn) {
		if (qtn instanceof JoinNode) {
			qtn = new ERJoinChooser((JoinNode) qtn).optimize();
		} else {
			for (int i = 0; i < qtn.getChildren().size(); i++) {
				PlanNode sub = qtn.getChildren().get(i);
				qtn.getChildren().set(i, optimize(sub));
			}
		}
		return qtn;
	}
}
