package io.mycat.plan.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import io.mycat.config.ErrorCode;
import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.ItemInt;
import io.mycat.plan.common.item.function.operator.ItemBoolFunc2;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncGe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncGt;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLt;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncNe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncStrictEqual;
import io.mycat.plan.common.item.function.operator.logic.ItemCondAnd;
import io.mycat.plan.common.item.function.operator.logic.ItemCondOr;
import io.mycat.plan.common.item.subquery.ItemInSubselect;
import io.mycat.plan.common.item.subquery.ItemSubselect;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.util.FilterUtils;

public class SubQueryPreProcessor {
	private final static String AUTONAME = "autosubgenrated0";
	private final static String AUTOALIAS = "autoalias_";

	public static PlanNode optimize(PlanNode qtn) {
		MergeHavingFilter.optimize(qtn);
		qtn = findComparisonsSubQueryToJoinNode(qtn);
		return qtn;
	}

	/**
	 * 处理下 = in的子查询转化为join
	 * http://dev.mysql.com/doc/refman/5.0/en/comparisons-using-subqueries.html
	 */
	private static PlanNode findComparisonsSubQueryToJoinNode(PlanNode qtn) {
		for (int i = 0; i < qtn.getChildren().size(); i++) {
			PlanNode child = qtn.getChildren().get(i);
			qtn.getChildren().set(i, findComparisonsSubQueryToJoinNode((PlanNode) child));
		}

		SubQueryAndFilter find = new SubQueryAndFilter();
		find.query = qtn;
		find.filter = null;
		Item where = qtn.getWhereFilter();
		SubQueryAndFilter result = buildSubQuery(find, where);
		if (result != find) {
			// 如果出现filter，代表where条件中没有组合条件，只有单自查询的条件，直接替换即可
			result.query.query(result.filter);
			qtn.query(null);
			// 修改了result.filter哦归属，需要重新build
			result.query.setUpFields();
			return result.query;
		} else {
			return qtn; // 没变化
		}
	}

	private static class SubQueryAndFilter {

		PlanNode query; // 子查询可能会改变query节点为join node
		Item filter; // 子查询带上来的filter
	}

	private static SubQueryAndFilter buildSubQuery(SubQueryAndFilter qtn, Item filter) {
		if (filter == null)
			return qtn;
		if (!filter.withSubQuery) {
			qtn.filter = filter;
		} else if (filter instanceof ItemCondOr) {
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support 'or' when condition subquery");
		} else if (filter instanceof ItemCondAnd) {
			ItemCondAnd andFilter = (ItemCondAnd) filter;
			for (int index = 0; index < andFilter.getArgCount(); index++) {
				SubQueryAndFilter result = buildSubQuery(qtn, andFilter.arguments().get(index));
				if (result != qtn) {
					if (result.filter == null) {
						result.filter = new ItemInt(1);
					}
					andFilter.arguments().set(index, result.filter);
					qtn = result;
				}
			}
			qtn.filter = andFilter;
		} else {
			Item leftColumn;
			PlanNode query;
			boolean isNotIn = false;
			boolean neeEexchange = false;
			if (filter instanceof ItemFuncEqual || filter instanceof ItemFuncGt || filter instanceof ItemFuncGe
					|| filter instanceof ItemFuncLt || filter instanceof ItemFuncLe || filter instanceof ItemFuncNe
					|| filter instanceof ItemFuncStrictEqual) {
				ItemBoolFunc2 eqFilter = (ItemBoolFunc2) filter;
				Item arg0 = eqFilter.arguments().get(0);
				Item arg1 = eqFilter.arguments().get(1);
				boolean arg0IsSubQuery = arg0.type().equals(ItemType.SUBSELECT_ITEM);
				boolean arg1IsSubQuery = arg1.type().equals(ItemType.SUBSELECT_ITEM);
				if (arg0IsSubQuery && arg1IsSubQuery) {
					throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
							"left and right both condition subquery,not supported...");
				}
				neeEexchange = arg0IsSubQuery;
				leftColumn = arg0IsSubQuery ? arg1 : arg0;
				query = arg0IsSubQuery ? ((ItemSubselect) arg0).getPlanNode() : ((ItemSubselect) arg1).getPlanNode();
			} else if (filter instanceof ItemInSubselect) {
				ItemInSubselect inSub = (ItemInSubselect) filter;
				leftColumn = inSub.getLeftOprand();
				query = inSub.getPlanNode();
				isNotIn = inSub.isNeg();
			} else {
				throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support subquery of:" + filter.type());
			}
			query = findComparisonsSubQueryToJoinNode(query);
			if (StringUtils.isEmpty(query.getAlias()))
				query.alias(AUTOALIAS + query.getPureName());
			if (query.getColumnsSelected().size() != 1)
				throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "only support subquery of one column");
			query.setSubQuery(true).setDistinct(true);
			List<Item> newSelects = qtn.query.getColumnsSelected();
			SubQueryAndFilter result = new SubQueryAndFilter();
			Item rightColumn = query.getColumnsSelected().get(0);
			qtn.query.setColumnsSelected(new ArrayList<Item>());
			String rightJoinName = rightColumn.getAlias();
			// add 聚合函数类型的支持
			if (StringUtils.isEmpty(rightJoinName)) {
				if (rightColumn instanceof ItemField) {
					rightJoinName = rightColumn.getItemName();
				} else {
					rightColumn.setAlias(AUTONAME);
					rightJoinName = AUTONAME;
				}
			}

			ItemField rightJoinColumn = new ItemField(null, query.getAlias(), rightJoinName);
			// left column的table名称需要改变
			result.query = new JoinNode(qtn.query, query);
			// 保留原sql至新的join节点
			result.query.setSql(qtn.query.getSql());
			qtn.query.setSql(null);
			result.query.select(newSelects);
			if (!qtn.query.getOrderBys().isEmpty()) {
				List<Order> orderBys = new ArrayList<Order>();
				orderBys.addAll(qtn.query.getOrderBys());
				result.query.setOrderBys(orderBys);
				qtn.query.getOrderBys().clear();
			}
			if (!qtn.query.getGroupBys().isEmpty()) {
				List<Order> groupBys = new ArrayList<Order>();
				groupBys.addAll(qtn.query.getGroupBys());
				result.query.setGroupBys(groupBys);
				qtn.query.getGroupBys().clear();
			}
			if (qtn.query.getLimitFrom() != -1) {
				result.query.setLimitFrom(qtn.query.getLimitFrom());
				qtn.query.setLimitFrom(-1);
			}
			if (qtn.query.getLimitTo() != -1) {
				result.query.setLimitTo(qtn.query.getLimitTo());
				qtn.query.setLimitTo(-1);
			}
			if (isNotIn) {
				((JoinNode) result.query).setLeftOuterJoin().setNotIn(true);
			} else {
				Item joinFrilter = null;
				if (((filter instanceof ItemFuncGt) && !neeEexchange)
						|| ((filter instanceof ItemFuncLt) && neeEexchange)) {
					joinFrilter = FilterUtils.GreaterThan(leftColumn, rightJoinColumn);
				} else if (((filter instanceof ItemFuncLt) && !neeEexchange)
						|| ((filter instanceof ItemFuncGt) && neeEexchange)) {
					joinFrilter = FilterUtils.LessThan(leftColumn, rightJoinColumn);
				} else if (((filter instanceof ItemFuncGe) && !neeEexchange)
						|| ((filter instanceof ItemFuncLe) && neeEexchange)) {
					joinFrilter = FilterUtils.GreaterEqual(leftColumn, rightJoinColumn);
				} else if (((filter instanceof ItemFuncLe) && !neeEexchange)
						|| ((filter instanceof ItemFuncGe) && neeEexchange)) {
					joinFrilter = FilterUtils.LessEqual(leftColumn, rightJoinColumn);
				} else if (filter instanceof ItemFuncNe) {
					joinFrilter = FilterUtils.NotEqual(leftColumn, rightJoinColumn);
				} else {
					//equal or in
					joinFrilter = FilterUtils.equal(leftColumn, rightJoinColumn);
				}
				((JoinNode) result.query).query(joinFrilter);
				result.filter = joinFrilter;
			}
			if (qtn.query.getAlias() == null && qtn.query.getSubAlias() == null) {
				result.query.setAlias(qtn.query.getPureName());
			} else {
				String queryAlias = qtn.query.getAlias();
				qtn.query.alias(null);
				if(queryAlias == null)
					queryAlias = qtn.query.getSubAlias();
				result.query.setAlias(queryAlias);
//				refreshItemTable(leftColumn, queryAlias);
			}
			result.query.setUpFields();
			return result;
		}
		return qtn;
	}

}
