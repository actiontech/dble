package io.mycat.plan.optimizer;

import java.util.HashSet;
import java.util.Set;

import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.util.PlanUtil;

public class GlobalTableProcessor {

	public static PlanNode optimize(PlanNode qtn) {
		initGlobalStatus(qtn);
		return qtn;
	}
	/**
	 * @param tn
	 * @return true:parent node有可能是global;false parent node没有可能是global
	 */
	private static boolean initGlobalStatus(PlanNode tn) {
		if (tn == null || tn.type() == PlanNodeType.TABLE)
			return true;
		boolean status = true;

		for (PlanNode child : tn.getChildren()) {
			boolean ret = initGlobalStatus(child);
			if (status) {
				status = ret;
			}
		}
		if (PlanUtil.isERNode(tn)) {
			// 是erjoin，只能算一个unglobaltable
			tn.setUnGlobalTableCount(1);
			Set<String> newSet = new HashSet<String>();
			newSet.addAll(tn.getReferedTableNodes().get(0).getNoshardNode());
			tn.setNoshardNode(newSet);
		} else {
			int unGlobalCount = 0;
			for (PlanNode tnChild : tn.getChildren()) {
				if (tnChild != null) {
					if (tn.getNoshardNode() == null) {
						if (tnChild.getNoshardNode() != null) {
							Set<String> parentSet = new HashSet<String>();
							parentSet.addAll(tnChild.getNoshardNode());
							tn.setNoshardNode(parentSet);
						}
					}
					if (tn.getNoshardNode() != null) {
						tn.getNoshardNode().retainAll(tnChild.getNoshardNode());
					}
				}
				unGlobalCount += tnChild.getUnGlobalTableCount();
			}
			tn.setUnGlobalTableCount(unGlobalCount);
			if (tn.getNoshardNode() != null && tn.getNoshardNode().size() == 0) {
				tn.setNoshardNode(null);
			}
			if (!status) {
				tn.setNoshardNode(null);
				return status;
			}
			if (unGlobalCount == 1 && tn instanceof JoinNode) {// joinNode
				JoinNode jn = (JoinNode) tn;
				if (jn.isNotIn()) {
					tn.setNoshardNode(null);
					status = false;
				} else if (jn.isInnerJoin()) {
					if (!isGlobalTableBigEnough(jn)) {
						tn.setNoshardNode(null);
						status = false;
					}
				} else {
					// left join
					PlanNode left = jn.getLeftNode();
					if (left.getUnGlobalTableCount() == 0) {// 左边是global，leftjoin不下发
						tn.setNoshardNode(null);
						status = false;
					} else if (left.type() == PlanNode.PlanNodeType.TABLE || !PlanUtil.isERNode(left)) {
						if (!isGlobalTableBigEnough(jn)) {
							tn.setNoshardNode(null);
							status = false;
						}
					} else {
						// 左边既不是单体表,也不是ER表
						tn.setNoshardNode(null);
						status = false;
					}
				}
			} else if (unGlobalCount != 0) {
				tn.setNoshardNode(null);
				status = false;
			}
		}
		return status;
	}
	private static boolean isGlobalTableBigEnough(JoinNode jn){
		PlanNode left = jn.getLeftNode();
		PlanNode right = jn.getRightNode();
		PlanNode global,noraml;
		if (left.getUnGlobalTableCount() == 0) {
			global= left;
			noraml= right;
		}else{
			global= right;
			noraml= left;
		}
		Set<String> result = new HashSet<String>();
		result.addAll(global.getNoshardNode());
		Set<String> normalSet = noraml.getNoshardNode();
		result.retainAll(normalSet);
		if (result.size() != normalSet.size()) {
			return false;
		}
		return true;
	}
}
