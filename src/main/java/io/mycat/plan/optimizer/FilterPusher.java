package io.mycat.plan.optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.MergeNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;

/**
 * 将filter进行下推
 * 
 * <pre>
 * a. 如果条件中包含||条件则暂不优化，下推时会导致语义不正确 b. 如果条件中的column/value包含function，也不做下推
 * (比较麻烦，需要递归处理函数中的字段信息，同时检查是否符合下推条件，先简答处理) c.
 * 如果条件中的column/value中的字段来自于子节点的函数查询，也不做下推
 * 
 * 几种场景： 1. where条件尽可能提前到叶子节点，同时提取出joinFilter 处理类型： JoinNode/QueryNode
 * 注意点：JoinNode如果是outter节点，则不能继续下推
 * 
 * 如： tabl1.join(table2).query(
 * "table1.id>5 && table2.id<10 && table1.name = table2.name") 优化成:
 * table1.query("table1.id>5").join(table2.query("table2.id<10").on(
 * "table1.name = table2.name")
 * 
 * 如: table1.join(table2).query("table1.id = table2.id")
 * 优化成：table1.join(table2).on("table1.id = table2.id")
 * 
 * 2. join中的非字段列条件，比如column = 1的常量关系，提前到叶子节点 处理类型：JoinNode 注意点：
 * 
 * 如： tabl1.join(table2).on("table1.id>5&&table2.id<10") 优化成:
 * table1.query("table1.id>5").join(table2.query("table2.id<10")) t但如果条件中包含
 * 
 * 3. join filter中的字段进行条件推导到左/右的叶子节点上，在第1和第2步优化中同时处理 处理类型：JoinNode
 * 
 * 如: table.join(table2).on(
 * "table1.id = table2.id and table1.id>5 && table2.id<10") 优化成：table1.query(
 * "table1.id>5 && table1.id<10").join(table2.query(
 * "table2.id>5 && table2.id<10")).on("table1.id = table2.id")
 */
public class FilterPusher {

	/**
	 * 详细优化见类描述 {@linkplain FilterPusher}
	 */
	public static PlanNode optimize(PlanNode qtn) {
		mergeJoinOnFilter(qtn);
		qtn = pushJoinOnFilter(qtn);
		qtn = pushFilter(qtn, new ArrayList<Item>());
		return qtn;
	}

	/**
	 * 将inner join中可合并的otheron条件合并到where
	 * 
	 * @param qtn
	 * @return
	 */
	private static void mergeJoinOnFilter(PlanNode qtn) {
		if (PlanUtil.isGlobalOrER(qtn))
			return;
		if (qtn.type().equals(PlanNodeType.JOIN) && ((JoinNode) qtn).isInnerJoin()) {
			JoinNode jn = (JoinNode) qtn;
			Item otherJoinOn = jn.getOtherJoinOnFilter();
			jn.setOtherJoinOnFilter(null);
			jn.query(FilterUtils.and(otherJoinOn, jn.getWhereFilter()));
		}
		for (PlanNode child : qtn.getChildren()) {
			mergeJoinOnFilter(child);
		}
	}

	private static PlanNode pushFilter(PlanNode qtn, List<Item> DNFNodeToPush) {
		List<Item> subHavingList = new ArrayList<Item>();
		for (Item filter : DNFNodeToPush) {
			if (filter.withSumFunc) {
				subHavingList.add(filter);
			}
		}
		if (!subHavingList.isEmpty()) {
			qtn.having(FilterUtils.and(qtn.getHavingFilter(), FilterUtils.and(subHavingList)));
			DNFNodeToPush.removeAll(subHavingList);
		}

		// 如果是根节点，接收filter做为where条件,否则继续合并当前where条件，然后下推
		if (qtn.getChildren().isEmpty() || PlanUtil.isGlobalOrER(qtn)) {
			Item node = FilterUtils.and(DNFNodeToPush);
			if (node != null) {
				qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
			}
			return qtn;
		}

		Item filterInWhere = qtn.getWhereFilter();
		if (filterInWhere != null) {
			List<Item> splits = FilterUtils.splitFilter(filterInWhere);
			qtn.query(null);
			DNFNodeToPush.addAll(splits);
		}

		if (qtn.type() == PlanNodeType.QUERY) {
			refreshPdFilters(qtn, DNFNodeToPush);
			PlanNode child = pushFilter(qtn.getChild(), DNFNodeToPush);
			((QueryNode) qtn).setChild(child);
		} else if (qtn.type() == PlanNodeType.JOIN) {
			JoinNode jn = (JoinNode) qtn;
			List<Item> DNFNodetoPushToLeft = new LinkedList<Item>();
			List<Item> DNFNodetoPushToRight = new LinkedList<Item>();
			List<Item> leftCopyedPushFilters = new LinkedList<Item>();
			List<Item> rightCopyedPushFilters = new LinkedList<Item>();
			List<Item> DNFNodeToCurrent = new LinkedList<Item>();

			PlanUtil.findJoinKeysAndRemoveIt(DNFNodeToPush, jn);
			for (Item filter : DNFNodeToPush) {
				// ex. 1 = -1
				if (filter.getReferTables().size() == 0) {
					DNFNodetoPushToLeft.add(filter);
					DNFNodetoPushToRight.add(filter);
					continue;
				}
				if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
					DNFNodetoPushToLeft.add(filter);
				} else if (PlanUtil.canPush(filter, jn.getRightNode(), jn)) {
					DNFNodetoPushToRight.add(filter);
				} else {
					DNFNodeToCurrent.add(filter);
				}
			}
			if (jn.isInnerJoin() || jn.isLeftOuterJoin() || jn.isRightOuterJoin()) {
				// 将左条件的表达式，推导到join filter的右条件上
				rightCopyedPushFilters
						.addAll(copyFilterToJoinOnColumns(DNFNodetoPushToLeft, jn.getLeftKeys(), jn.getRightKeys()));

				// 将右条件的表达式，推导到join filter的左条件上
				leftCopyedPushFilters
						.addAll(copyFilterToJoinOnColumns(DNFNodetoPushToRight, jn.getRightKeys(), jn.getLeftKeys()));
			}

			// 针对不能下推的，合并到当前的where
			Item node = FilterUtils.and(DNFNodeToCurrent);
			if (node != null) {
				qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
			}

			if (jn.isInnerJoin() || jn.isLeftOuterJoin() || jn.isRightOuterJoin()) {
				if (jn.isLeftOuterJoin()) {
					// left join，把right join下推之后，还得把right join的条件留下来
					jn.query(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(DNFNodetoPushToRight)));
				}
				if (jn.isRightOuterJoin()) {
					// right join，把right join下推之后，还得把left join的条件留下来
					jn.query(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(DNFNodetoPushToLeft)));
				}
				// 合并起来
				DNFNodetoPushToRight.addAll(rightCopyedPushFilters);
				DNFNodetoPushToLeft.addAll(leftCopyedPushFilters);

				refreshPdFilters(jn, DNFNodetoPushToLeft);
				refreshPdFilters(jn, DNFNodetoPushToRight);
				jn.setLeftNode(pushFilter(jn.getLeftNode(), DNFNodetoPushToLeft));
				jn.setRightNode(pushFilter(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight));
			} else {
				if (DNFNodeToPush != null && !DNFNodeToPush.isEmpty()) {
					jn.query(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(DNFNodeToPush)));
				}
			}
			return jn;
		} else if (qtn.type() == PlanNodeType.MERGE) {
			// union语句的where条件可以下推，但是要替换成相应的child节点的过滤条件

			Item node = FilterUtils.and(DNFNodeToPush);
			if (node != null) {
				qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
			}
			Item mergeWhere = qtn.getWhereFilter();
			// 加速优化，将merge的条件挨个下推
			qtn.query(null);
			List<Item> pushFilters = PlanUtil.getPushItemsToUnionChild((MergeNode) qtn, mergeWhere,
					((MergeNode) qtn).getColIndexs());
			List<PlanNode> childs = qtn.getChildren();
			for (int index = 0; index < childs.size(); index++) {
				PlanNode child = childs.get(index);
				if (pushFilters != null) {
					Item pushFilter = pushFilters.get(index);
					child.query(FilterUtils.and(child.getWhereFilter(), pushFilter));
				}
				FilterPusher.optimize(child);
			}
			return qtn;
		}

		return qtn;
	}

	/**
	 * inner join的other join on在FilterPre时会被优化成where，只有left join有这个可能性 Left
	 * join时， select * from t1 left jion t2 on t1.id=t2.id and t1.id = 10 and
	 * t2.name = 'aaa' 可以将t2.id=10和t2.name='aaa'进行下推
	 * 
	 */
	private static PlanNode pushJoinOnFilter(PlanNode qtn) {
		if (PlanUtil.isGlobalOrER(qtn))
			return qtn;
		if (qtn.type().equals(PlanNodeType.JOIN)) {
			JoinNode jn = (JoinNode) qtn;
			Item otherJoinOn = jn.getOtherJoinOnFilter();
			if (jn.isLeftOuterJoin() && otherJoinOn != null) {
				List<Item> pushToRightNode = new ArrayList<Item>();
				List<Item> splitedFilters = FilterUtils.splitFilter(otherJoinOn);
				for (Item filter : splitedFilters) {
					if (filter.getReferTables().isEmpty())
						pushToRightNode.add(filter);
					else if (PlanUtil.canPush(filter, jn.getRightNode(), jn))
						pushToRightNode.add(filter);
					else if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
						Item copyedFilter = copyFilterToJoinOnColumns(filter, jn.getRightKeys(), jn.getLeftKeys());
						if (copyedFilter != null)
							pushToRightNode.add(copyedFilter);
					} else
						continue;
				}
				if (!pushToRightNode.isEmpty()) {
					splitedFilters.removeAll(pushToRightNode);
					Item newOtherJoinOn = FilterUtils.and(splitedFilters);
					jn.setOtherJoinOnFilter(newOtherJoinOn);
					refreshPdFilters(jn, pushToRightNode);
					List<Item> subHavingList = new ArrayList<Item>();
					List<Item> subWhereList = new ArrayList<Item>();
					for (Item filter : pushToRightNode) {
						if (filter.withSumFunc) {
							subHavingList.add(filter);
						} else {
							subWhereList.add(filter);
						}
					}
					Item subHaving = FilterUtils.and(subHavingList);
					Item subWhere = FilterUtils.and(subWhereList);
					jn.getRightNode().having(FilterUtils.and(jn.getRightNode().getHavingFilter(), subHaving));
					jn.getRightNode().setWhereFilter(FilterUtils.and(jn.getRightNode().getWhereFilter(), subWhere));
				}
			}
		}
		for (PlanNode child : qtn.getChildren())
			pushJoinOnFilter(child);
		return qtn;
	}

	/**
	 * 将连接列上的约束复制到目标节点内
	 * 
	 * @param DNF
	 *            要复制的DNF filter
	 * @param qnColumns
	 *            源节点的join字段
	 * @param otherColumns
	 */
	private static List<Item> copyFilterToJoinOnColumns(List<Item> DNF, List<Item> qnColumns, List<Item> otherColumns) {
		List<Item> newIFilterToPush = new LinkedList<Item>();
		for (Item filter : DNF) {
			Item newFilter = copyFilterToJoinOnColumns(filter, qnColumns, otherColumns);
			if (newFilter != null)
				newIFilterToPush.add(newFilter);
		}
		return newIFilterToPush;
	}

	private static Item copyFilterToJoinOnColumns(Item filter, List<Item> qnColumns, List<Item> otherColumns) {
		if (filter.type().equals(ItemType.FUNC_ITEM) || filter.type().equals(ItemType.COND_ITEM)) {
			ItemFunc newFilter = replaceFunctionArg((ItemFunc) filter, qnColumns, otherColumns);
			if (newFilter != null)
				return newFilter;
		}
		return null;
	}

	private static void refreshPdFilters(PlanNode qtn, List<Item> filters) {
		for (int index = 0; index < filters.size(); index++) {
			Item toPsFilter = filters.get(index);
			Item pdFilter = PlanUtil.pushDownItem(qtn, toPsFilter);
			filters.set(index, pdFilter);
		}
	}

	/**
	 * 将function中的涉及到c1的参数替换成c2
	 * 
	 * @param f
	 * @param sels1
	 * @param sels2
	 * @return 如果f中还存在非sels1的selectable，返回null
	 */
	public static ItemFunc replaceFunctionArg(ItemFunc f, List<Item> sels1, List<Item> sels2) {
		ItemFunc ret = (ItemFunc) f.cloneStruct();
		for (int index = 0; index < ret.getArgCount(); index++) {
			Item arg = ret.arguments().get(index);
			if (arg instanceof ItemFunc) {
				ItemFunc newfArg = replaceFunctionArg((ItemFunc) arg, sels1, sels2);
				if (newfArg == null)
					return null;
				else
					ret.arguments().set(index, newfArg);
			} else if (arg instanceof ItemField) {
				int tmpIndex = sels1.indexOf(arg);
				if (tmpIndex < 0) {
					return null;
				} else {
					Item newArg = sels2.get(tmpIndex);
					ret.arguments().set(index, newArg.cloneStruct());
				}
			} else {
				// do nothing;
			}
		}
		ret.setPushDownName(null);
		PlanUtil.refreshReferTables(ret);
		return ret;
	}

}
