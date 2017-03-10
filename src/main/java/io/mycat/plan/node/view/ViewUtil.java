package io.mycat.plan.node.view;

import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.util.PlanUtil;

/**
 * Some function from mysql source code
 * 
 * @author ActionTech
 * 
 */
public class ViewUtil {

	/**
	 * Check whether the merging algorithm can be used on this VIEW see
	 * LEX::can_be_merged() in sql_lex.cc
	 * 
	 * @param viewSelNode
	 *            view的真实selectnode
	 * @return FALSE - only temporary table algorithm can be used TRUE - merge
	 *         algorithm can be used
	 */
	public static boolean canBeMerged(PlanNode viewSelNode) {
		if (viewSelNode.type() == PlanNodeType.NONAME)
			return true;
		boolean selectsAllowMerge = viewSelNode.type() != PlanNodeType.MERGE;
		// TODO as the same as LEX::can_be_merged();
		boolean existAggr = PlanUtil.existAggr(viewSelNode);
		return selectsAllowMerge && viewSelNode.getReferedTableNodes().size() >= 1 && !existAggr;
	}
}
