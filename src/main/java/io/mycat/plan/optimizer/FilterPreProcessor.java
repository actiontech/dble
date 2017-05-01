package io.mycat.plan.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemInt;
import io.mycat.plan.common.item.function.ItemFunc.Functype;
import io.mycat.plan.common.item.function.operator.ItemBoolFunc2;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncGe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncGt;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLe;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncLt;
import io.mycat.plan.common.item.function.operator.logic.ItemCond;
import io.mycat.plan.common.item.function.operator.logic.ItemCondAnd;
import io.mycat.plan.common.item.function.operator.logic.ItemCondOr;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;

/**
 * http://dev.mysql.com/doc/refman/5.7/en/where-optimizations.html
 * 
 * @author ActionTech
 * @CreateTime Mar 16, 2016
 */
public class FilterPreProcessor {

	public static PlanNode optimize(PlanNode qtn) {
		MergeHavingFilter.optimize(qtn);
		qtn = preProcess(qtn);
		return qtn;
	}

	

	private static PlanNode preProcess(PlanNode qtn) {
		qtn.having(processFilter(qtn.getHavingFilter()));
		qtn.query(processFilter(qtn.getWhereFilter()));
		if (qtn instanceof JoinNode) {
			JoinNode jn = (JoinNode) qtn;
			for (int i = 0; i < ((JoinNode) qtn).getJoinFilter().size(); i++) {
				processFilter(jn.getJoinFilter().get(i));
			}
			jn.setOtherJoinOnFilter(processFilter(jn.getOtherJoinOnFilter()));
		}
		for (PlanNode child : qtn.getChildren()) {
			preProcess((PlanNode) child);
		}
		return qtn;
	}

	private static Item processFilter(Item root) {
		if (root == null) {
			return null;
		}

		root = shortestFilter(root);
		root = processOneFilter(root); // 做一下转换处理
		root = convertOrToIn(root);
		return root;
	}

	/**
	 * 将0=1/1=1/true的恒等式进行优化
	 */
	private static Item shortestFilter(Item root) {
		if (root == null)
			return root;
		if (root.canValued()) {
			boolean value = root.valBool();
			if (value)
				return new ItemInt(1);
			else
				return new ItemInt(0);
		} else if (root.type().equals(ItemType.COND_ITEM)) {
			ItemCond cond = (ItemCond) root;
			for (int index = 0; index < cond.getArgCount(); index++) {
				Item shortedsub = shortestFilter(cond.arguments().get(index));
				cond.arguments().set(index, shortedsub);
			}
			boolean isAnd = cond.functype().equals(Functype.COND_AND_FUNC);
			List<Item> newSubFilters = new ArrayList<Item>();
			for (Item sub : cond.arguments()) {
				if (sub == null)
					continue;
				if (sub.canValued()) {
					boolean value = sub.valBool();
					if (value == true && !isAnd)
						return new ItemInt(1);
					if (value == false && isAnd)
						return new ItemInt(0);
				} else {
					newSubFilters.add(sub);
				}
			}
			if (isAnd)
				return FilterUtils.and(newSubFilters);
			else
				return FilterUtils.or(newSubFilters);
		} else {
			return root;
		}
	}

	/**
	 * 尽量将const op column统一改为column op const这种
	 * 
	 * @param root
	 * @return
	 */
	private static Item processOneFilter(Item root) {
		if (root == null) {
			return null;
		}
		Item newRoot = root;
		if (root instanceof ItemBoolFunc2) {
			Item a = root.arguments().get(0);
			Item b = root.arguments().get(1);
			if (a.basicConstItem() && !b.basicConstItem()) {
				if (root instanceof ItemFuncGe) {
					newRoot = new ItemFuncLe(b, a);
				} else if (root instanceof ItemFuncGt) {
					newRoot = new ItemFuncLt(b, a);
				} else if (root instanceof ItemFuncLt) {
					newRoot = new ItemFuncGt(b, a);
				} else if (root instanceof ItemFuncLe) {
					newRoot = new ItemFuncGe(b, a);
				} else {
					root.arguments().set(1, a);
					root.arguments().set(0, b);
					root.setItemName(null);
				}
				newRoot.getReferTables().addAll(root.getReferTables());
			}
		} else if (root instanceof ItemCond) {
			ItemCond condfun = (ItemCond) root;
			List<Item> newArgs = new ArrayList<Item>();
			for (Item arg : condfun.arguments()) {
				Item newArg = processOneFilter(arg);
				if (newArg != null)
					newArgs.add(newArg);
			}
			if (condfun.functype().equals(Functype.COND_AND_FUNC))
				newRoot = FilterUtils.and(newArgs);
			else
				newRoot = FilterUtils.or(newArgs);
		}
		return newRoot;
	}

	/**
	 * 将单个的Logicalfilter【or】尽可能的转换成in
	 * 
	 * @param filter
	 */
	private static Item convertOrToIn(Item filter) {
		if (filter == null)
			return null;
		if (filter.type().equals(ItemType.COND_ITEM)) {
			if (filter instanceof ItemCondAnd) {
				ItemCondAnd andFilter = (ItemCondAnd) filter;
				for (int index = 0; index < andFilter.getArgCount(); index++) {
					andFilter.arguments().set(index, convertOrToIn(andFilter.arguments().get(index)));
				}
				andFilter.setItemName(null);
				PlanUtil.refreshReferTables(andFilter);
				return andFilter;
			} else {
				// or
				ItemCondOr orFilter = (ItemCondOr) filter;
				HashMap<Item, Set<Item>> inMap = new HashMap<Item, Set<Item>>();
				List<Item> newSubFilterList = new ArrayList<Item>();
				for (int index = 0; index < orFilter.getArgCount(); index++) {
					Item subFilter = orFilter.arguments().get(index);
					if (subFilter == null)
						continue;
					if (subFilter instanceof ItemFuncEqual) {
						Item a = ((ItemFuncEqual) subFilter).arguments().get(0);
						Item b = ((ItemFuncEqual) subFilter).arguments().get(1);
						if (!a.canValued() && b.canValued()) {
							if (!inMap.containsKey(a))
								inMap.put(a, new HashSet<Item>());
							inMap.get(a).add(b);
						}
					} else {
						Item subNew = convertOrToIn(subFilter);
						newSubFilterList.add(subNew);
					}
				}
				for (Item inKey : inMap.keySet()) {
					Set<Item> inValues = inMap.get(inKey);
					List<Item> args = new ArrayList<Item>();
					args.add(inKey);
					args.addAll(inValues);
					ItemFuncIn inItem = new ItemFuncIn(args, false);
					PlanUtil.refreshReferTables(inItem);
					newSubFilterList.add(inItem);
				}
				return FilterUtils.or(newSubFilterList);
			}
		}
		return filter;
	}

}