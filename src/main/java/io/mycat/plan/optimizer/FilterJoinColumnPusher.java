package io.mycat.plan.optimizer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.node.TableNode;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;
import io.mycat.route.parser.util.Pair;

/**
 * 只下推有ER关系可能的filter
 * 
 * @author chenzifei
 * 
 */
public class FilterJoinColumnPusher {

	public static PlanNode optimize(PlanNode qtn) {
		qtn = pushFilter(qtn, new ArrayList<Item>());
		return qtn;
	}

	private static PlanNode pushFilter(PlanNode qtn, List<Item> DNFNodeToPush) {
		// 如果是叶节点，接收filter做为where条件,否则继续合并当前where条件，然后下推
		if (qtn.getChildren().isEmpty()) {
			Item node = FilterUtils.and(DNFNodeToPush);
			if (node != null) {
				qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
			}
			return qtn;
		}

		Item filterInWhere = qtn.getWhereFilter();
		if (filterInWhere != null) {
			List<Item> splits = FilterUtils.splitFilter(filterInWhere);
			List<Item> nonJoinFilter = new ArrayList<Item>();
			for (Item filter : splits) {
				if (isPossibleERJoinColumnFilter(qtn, filter) == false) {
					nonJoinFilter.add(filter);
				} else {
					DNFNodeToPush.add((ItemFuncEqual) filter);
				}
			}
			qtn.query(FilterUtils.and(nonJoinFilter));// 清空where条件
		}

		// 无法完成下推的filters
		List<Item> DNFNodeToCurrent = new LinkedList<Item>();
		switch (qtn.type()) {
		case QUERY:
			refreshPdFilters(qtn, DNFNodeToPush);
			PlanNode child = pushFilter(qtn.getChild(), DNFNodeToPush);
			((QueryNode) qtn).setChild(child);
			break;
		case JOIN:
			JoinNode jn = (JoinNode) qtn;
			List<Item> DNFNodetoPushToLeft = new LinkedList<Item>();
			List<Item> DNFNodetoPushToRight = new LinkedList<Item>();
			PlanUtil.findJoinKeysAndRemoveIt(DNFNodeToPush, jn);
			for (Item filter : DNFNodeToPush) {
				if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
					DNFNodetoPushToLeft.add(filter);
				} else if (PlanUtil.canPush(filter, jn.getRightNode(), jn)) {
					DNFNodetoPushToRight.add(filter);
				} else {
					DNFNodeToCurrent.add(filter);
				}
			}
			// 针对不能下推的，合并到当前的where
			Item node = FilterUtils.and(DNFNodeToCurrent);
			if (node != null) {
				qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
			}
			if (jn.isInnerJoin()) {
				refreshPdFilters(jn, DNFNodetoPushToLeft);
				refreshPdFilters(jn, DNFNodetoPushToRight);
				pushFilter(jn.getLeftNode(), DNFNodetoPushToLeft);
				pushFilter(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight);
			} else if (jn.isLeftOuterJoin()) {
				refreshPdFilters(jn, DNFNodetoPushToLeft);
				pushFilter(jn.getLeftNode(), DNFNodetoPushToLeft);
				if (!DNFNodeToPush.isEmpty()) {
					jn.query(FilterUtils.and(DNFNodetoPushToRight)); // 在父节点完成filter，不能下推
				}
			} else if (jn.isRightOuterJoin()) {
				refreshPdFilters(jn, DNFNodetoPushToRight);
				pushFilter(((JoinNode) qtn).getRightNode(), DNFNodetoPushToRight);
				if (!DNFNodeToPush.isEmpty()) {
					jn.query(FilterUtils.and(DNFNodetoPushToLeft));// 在父节点完成filter，不能下推
				}
			} else {
				if (!DNFNodeToPush.isEmpty()) {
					jn.query(FilterUtils.and(DNFNodeToPush));
				}
			}
			break;
		case MERGE:
			List<PlanNode> children = qtn.getChildren();
			for (int index = 0; index < children.size(); index++) {
				pushFilter(children.get(index), new ArrayList<Item>());
			}
			break;
		default:
			break;
		}
		return qtn;
	}

	/**
	 * 是否是可能得ER关系Filter： 1.Filter必须是=关系 2.Filter必须是Column = Column
	 * 3.Filter的key和value必须来自于不同的两张表 ex:a.id=b.id true a.id=b.id+1 false
	 * 
	 * @param filter
	 * @return
	 */
	private static boolean isPossibleERJoinColumnFilter(PlanNode node, Item ifilter) {
		if (!(ifilter instanceof ItemFuncEqual))
			return false;
		ItemFuncEqual filter = (ItemFuncEqual) ifilter;
		Item column = filter.arguments().get(0);
		Item value = filter.arguments().get(1);
		if (column != null && column instanceof ItemField && value != null && value instanceof ItemField) {
			Pair<TableNode, ItemField> foundColumn = PlanUtil.findColumnInTableLeaf((ItemField) column, node);
			Pair<TableNode, ItemField> foundValue = PlanUtil.findColumnInTableLeaf((ItemField) value, node);
			if (foundColumn != null && foundValue != null) {
				String columnTable = foundColumn.getValue().getTableName();
				String valueTable = foundValue.getValue().getTableName();
				// 不是同一张表才可以
				return !StringUtils.equals(columnTable, valueTable);
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	private static void refreshPdFilters(PlanNode qtn, List<Item> filters) {
		for (int index = 0; index < filters.size(); index++) {
			Item toPsFilter = filters.get(index);
			Item pdFilter = PlanUtil.pushDownItem(qtn, toPsFilter);
			filters.set(index, pdFilter);
		}
	}

}
