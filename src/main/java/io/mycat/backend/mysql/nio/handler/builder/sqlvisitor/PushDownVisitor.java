package io.mycat.backend.mysql.nio.handler.builder.sqlvisitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.TableNode;

/**
 * 处理那种可以下推部分sql，又和global表的下推不同的node类型 单个table er关系表 非全global表的下退等等
 * 
 * @author ActionTech
 * 
 */
public class PushDownVisitor extends MysqlVisitor {

	/* 用来记录真正被下发下去的orderby列表 */
	private List<Order> pushDownOrderBy;

	public PushDownVisitor(PlanNode pushDownQuery, boolean isTopQuery) {
		super(pushDownQuery, isTopQuery);
		this.existUnPushDownGroup = pushDownQuery.existUnPushDownGroup();
		pushDownOrderBy = new ArrayList<Order>();
	}

	public void visit() {
		if (!visited) {
			replaceableSqlBuilder.clear();
			sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
			// 在已经visited的情况下，pushdownvisitor只要进行table名称的替换即可
			PlanNode.PlanNodeType i = query.type();
			if (i == PlanNode.PlanNodeType.TABLE) {
				visit((TableNode) query);

			} else if (i == PlanNode.PlanNodeType.JOIN) {
				visit((JoinNode) query);

			} else {
				throw new RuntimeException("not implement yet!");
			}
			visited = true;
		} else {
			// where的可替换仅针对tablenode，不可以迭代
			buildWhere(query);
		}
	}

	

	protected void visit(TableNode query) {
		if (query.isSubQuery() && !isTopQuery) {
			sqlBuilder.append(" ( ");
		}
		if (query.isSubQuery() || isTopQuery) {
			buildSelect(query);

			if (query.getTableName() == null)
				return;
			sqlBuilder.append(" from ");
		}
		// 需要根据是否是下划表进行计算，生成可替换的String
		buildTableName(query, sqlBuilder);
		if (query.isSubQuery() || isTopQuery) {
			buildWhere(query);
			buildGroupBy(query);
			// having中由于可能存在聚合函数，而聚合函数需要merge之后结果才能出来，所以需要自己进行计算
			buildOrderBy(query);
			buildLimit(query, sqlBuilder);
		}

		if (query.isSubQuery() && !isTopQuery) {
			sqlBuilder.append(" ) ");
			if (query.getAlias() != null) {
				sqlBuilder.append(" ").append(query.getAlias()).append(" ");
			}
		}
	}

	protected void visit(JoinNode join) {
		if (!isTopQuery) {
			sqlBuilder.append(" ( ");
		}
		if (join.isSubQuery() || isTopQuery) {
			buildSelect(join);
			sqlBuilder.append(" from ");
		}

		PlanNode left = join.getLeftNode();
		PlanNode right = join.getRightNode();
		MysqlVisitor leftVisitor = new GlobalVisitor(left, false);
		leftVisitor.visit();
		replaceableSqlBuilder.append(leftVisitor.getSql());
		sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
		if (join.getLeftOuter() && join.getRightOuter()) {
			throw new RuntimeException("full outter join 不支持");
		} else if (join.getLeftOuter() && !join.getRightOuter()) {
			sqlBuilder.append(" left");
		} else if (join.getRightOuter() && !join.getLeftOuter()) {
			sqlBuilder.append(" right");
		}

		sqlBuilder.append(" join ");
		MysqlVisitor rightVisitor = new GlobalVisitor(right, false);
		rightVisitor.visit();
		replaceableSqlBuilder.append(rightVisitor.getSql());
		sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
		StringBuilder joinOnFilterStr = new StringBuilder();
		boolean first = true;
		for (int i = 0; i < join.getJoinFilter().size(); i++) {
			Item filter = join.getJoinFilter().get(i);
			if (first) {
				sqlBuilder.append(" on ");
				first = false;
			} else
				joinOnFilterStr.append(" and ");
			joinOnFilterStr.append(filter);
		}

		if (join.getOtherJoinOnFilter() != null) {
			if (first) {
				first = false;
			} else {
				joinOnFilterStr.append(" and ");
			}
			joinOnFilterStr.append(join.getOtherJoinOnFilter());
		}
		sqlBuilder.append(joinOnFilterStr.toString());
		if (join.isSubQuery() || isTopQuery) {
			buildWhere(join);
			buildGroupBy(join);
			// having中由于可能存在聚合函数，而聚合函数需要merge之后结果才能出来，所以需要自己进行计算
			buildOrderBy(join);
			buildLimit(join, sqlBuilder);
		}

		if (!isTopQuery) {
			sqlBuilder.append(" ) ");
			if (join.getAlias() != null)
				sqlBuilder.append(" ").append(join.getAlias()).append(" ");
		}

	}

	protected void buildSelect(PlanNode query) {
		sqlBuilder.append("select ");
		List<Item> columns = query.getColumnsRefered();
		if (query.isDistinct()) {
			sqlBuilder.append("DISTINCT ");
		}
		for (Item col : columns) {
			if (existUnPushDownGroup && col.type().equals(ItemType.SUM_FUNC_ITEM))
				continue;
			if ((col.type().equals(ItemType.FUNC_ITEM) || col.type().equals(ItemType.COND_ITEM)) && col.withSumFunc)
				continue;
			String pdName = visitPushDownNameSel(col);
			if (StringUtils.isEmpty(pdName))// 重复列
				continue;
			if (col.type().equals(ItemType.SUM_FUNC_ITEM)) {
				ItemSum funCol = (ItemSum) col;
				String funName = funCol.funcName().toUpperCase();
				String colName = pdName;
				ItemSum.Sumfunctype i = funCol.sumType();
				if (i == ItemSum.Sumfunctype.AVG_FUNC) {
					String colNameSum = colName.replace(funName + "(", "SUM(");
					colNameSum = colNameSum.replace(getMadeAggAlias(funName), getMadeAggAlias("SUM"));
					String colNameCount = colName.replace(funName + "(", "COUNT(");
					colNameCount = colNameCount.replace(getMadeAggAlias(funName), getMadeAggAlias("COUNT"));
					sqlBuilder.append(colNameSum).append(",").append(colNameCount).append(",");
					continue;
				} else if (i == ItemSum.Sumfunctype.STD_FUNC || i == ItemSum.Sumfunctype.VARIANCE_FUNC) {
					String colNameCount = colName.replace(funName + "(", "COUNT(");
					colNameCount = colNameCount.replace(getMadeAggAlias(funName), getMadeAggAlias("COUNT"));
					String colNameSum = colName.replace(funName + "(", "SUM(");
					colNameSum = colNameSum.replace(getMadeAggAlias(funName), getMadeAggAlias("SUM"));
					String colNameVar = colName.replace(funName + "(", "VARIANCE(");
					colNameVar = colNameVar.replace(getMadeAggAlias(funName), getMadeAggAlias("VARIANCE"));
					sqlBuilder.append(colNameCount).append(",").append(colNameSum).append(",").append(colNameVar)
							.append(",");
					continue;
				}
			}
			sqlBuilder.append(pdName);
			sqlBuilder.append(",");
		}
		sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
	}

	protected void buildGroupBy(PlanNode query) {
		if (nodeHasGroupBy(query)) {
			// 可以下发整个group by的情形
			if (!existUnPushDownGroup) {
				if (!query.getGroupBys().isEmpty()) {
					sqlBuilder.append(" GROUP BY ");
					for (Order group : query.getGroupBys()) {
						// 记录下当前下推的结果集的排序
						pushDownOrderBy.add(group.copy());
						Item groupCol = group.getItem();
						String pdName = "";
						if (groupCol.basicConstItem())
							pdName = "'" + groupCol.toString() + "'";
						if (pdName.isEmpty())
							pdName = visitUnselPushDownName(groupCol, true);
						sqlBuilder.append(pdName).append(" ").append(group.getSortOrder()).append(",");
					}
					sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
				}
			} else {
				// 不可以下发group by的情况，转化为下发order
				pushDownOrderBy.addAll(query.getGroupBys());
				if (pushDownOrderBy.size() > 0) {
					sqlBuilder.append(" ORDER BY ");
					for (Order order : pushDownOrderBy) {
						Item orderSel = order.getItem();
						String pdName = "";
						if (orderSel.basicConstItem())
							pdName = "'" + orderSel.toString() + "'";
						if (pdName.isEmpty())
							pdName = visitUnselPushDownName(orderSel, true);
						sqlBuilder.append(pdName).append(" ").append(order.getSortOrder()).append(",");
					}
					sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
				}
			}
		}
	}

	protected void buildOrderBy(PlanNode query) {
		/* 由于有groupby时，在merge的时候需要根据groupby的列进行排序merge，所以有groupby时不能下发order */
		boolean realPush = query.getGroupBys().isEmpty();
		if (query.getOrderBys().size() > 0) {
			if (realPush)
				sqlBuilder.append(" ORDER BY ");
			for (Order order : query.getOrderBys()) {
				Item orderByCol = order.getItem();
				String pdName = "";
				if (orderByCol.basicConstItem())
					pdName = "'" + orderByCol.toString() + "'";
				if (pdName.isEmpty())
					pdName = visitUnselPushDownName(orderByCol, true);
				if (realPush) {
					pushDownOrderBy.add(order.copy());
					sqlBuilder.append(pdName).append(" ").append(order.getSortOrder()).append(",");
				}
			}
			if (realPush)
				sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
		}
	}

	protected void buildLimit(PlanNode query, StringBuilder sb) {
		/* groupby和limit共存时，是不可以下发limit的 */
		if (query.getGroupBys().isEmpty() && !existUnPushDownGroup) {
			/* 只有order by可以下发时，limit才可以下发 */
			if (query.getLimitFrom() != -1 && query.getLimitTo() != -1) {
				sb.append(" LIMIT ").append(query.getLimitFrom() + query.getLimitTo());
			}
		}
	}


	/* -------------------------- help method ------------------------ */

	/* 判断node是否需要groupby */
	public static boolean nodeHasGroupBy(PlanNode node) {
		return (node.sumFuncs.size() > 0 || node.getGroupBys().size() > 0);
	}

	@Override
	protected String visitPushDownNameSel(Item item) {
		String orgPushDownName = item.getItemName();
		if (item.type().equals(ItemType.FIELD_ITEM)) {
			orgPushDownName = "`" + item.getTableName() + "`.`" + orgPushDownName + "`";
		}
		String pushAlias = null;
		if (pushNameMap.containsKey(orgPushDownName)) {
			// 重复的列不下发
			item.setPushDownName(pushNameMap.get(orgPushDownName));
			return null;
		}
		if (item.type().equals(ItemType.SUM_FUNC_ITEM)) {
			// 聚合函数添加别名，但是需要表示出是哪个聚合函数
			String aggName = ((ItemSum) item).funcName().toUpperCase();
			pushAlias = getMadeAggAlias(aggName) + getRandomAliasName();
		} else if (item.getAlias() != null) {
			pushAlias = item.getAlias();
			if (pushAlias.startsWith(Item.FNAF))
				pushAlias = getRandomAliasName();
		} else if (orgPushDownName.length() > MAX_COL_LENGTH) {
			pushAlias = getRandomAliasName();
		} else if (isTopQuery && !item.type().equals(ItemType.FIELD_ITEM)) {
			pushAlias = getRandomAliasName();
		}
		if (pushAlias == null) {
			if (item.type().equals(ItemType.FIELD_ITEM)) {
				pushNameMap.put(orgPushDownName, null);
			} else {
				item.setPushDownName(orgPushDownName);
				pushNameMap.put(orgPushDownName, orgPushDownName);
			}
		} else {
			item.setPushDownName(pushAlias);
			pushNameMap.put(orgPushDownName, pushAlias);
		}
		
		if (pushAlias == null)
			return orgPushDownName;
		else
			return orgPushDownName + " as `" + pushAlias + "`";
	}

}
