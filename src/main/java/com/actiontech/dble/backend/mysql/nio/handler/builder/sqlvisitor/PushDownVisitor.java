/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.TableNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * execute like single er tables ,global * normal table
 *
 * @author ActionTech
 */
public class PushDownVisitor extends MysqlVisitor {

    /* store order by list pushed down*/
    private List<Order> pushDownOrderBy;

    public PushDownVisitor(PlanNode pushDownQuery, boolean isTopQuery) {
        super(pushDownQuery, isTopQuery);
        this.existUnPushDownGroup = pushDownQuery.existUnPushDownGroup();
        pushDownOrderBy = new ArrayList<>();
    }

    public void visit() {
        if (!visited) {
            replaceableSqlBuilder.clear();
            sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
            // if visited,push down visitor need just replace the name
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
            // where's replaceable is just for table node
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
        buildTableName(query, sqlBuilder);
        if (query.isSubQuery() || isTopQuery) {
            buildWhere(query);
            buildGroupBy(query);
            // having may contains aggregate function, so it need to calc by middle-ware
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
        MysqlVisitor leftVisitor = new GlobalVisitor(left, false);
        leftVisitor.visit();
        replaceableSqlBuilder.append(leftVisitor.getSql());
        sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
        if (join.getLeftOuter() && join.getRightOuter()) {
            throw new RuntimeException("not supported for full outer join");
        } else if (join.getLeftOuter() && !join.getRightOuter()) {
            sqlBuilder.append(" left");
        } else if (join.getRightOuter() && !join.getLeftOuter()) {
            sqlBuilder.append(" right");
        }

        sqlBuilder.append(" join ");

        PlanNode right = join.getRightNode();
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
            // having may contains aggregate function, so it need to calc by middle-ware
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
            if (existUnPushDownGroup && col.type().equals(Item.ItemType.SUM_FUNC_ITEM))
                continue;
            if ((col.type().equals(Item.ItemType.FUNC_ITEM) || col.type().equals(Item.ItemType.COND_ITEM)) && col.isWithSumFunc())
                continue;
            String pdName = visitPushDownNameSel(col);
            if (StringUtils.isEmpty(pdName))// it's null when duplicate column
                continue;
            if (col.type().equals(Item.ItemType.SUM_FUNC_ITEM)) {
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
                    sqlBuilder.append(colNameCount).append(",").append(colNameSum).append(",").append(colNameVar).append(",");
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
            // push down group by
            if (!existUnPushDownGroup) {
                if (!query.getGroupBys().isEmpty()) {
                    sqlBuilder.append(" GROUP BY ");
                    for (Order group : query.getGroupBys()) {
                        // store the order by's order
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
                // change to push down order by
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
        /* if group by exists,it must merge as "group by"'s order,so don't push down order */
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
        /* both group by and limit are exist, don't push down limit */
        if (query.getGroupBys().isEmpty() && !existUnPushDownGroup) {
            if (query.getLimitFrom() != -1 && query.getLimitTo() != -1) {
                sb.append(" LIMIT ").append(query.getLimitFrom() + query.getLimitTo());
            }
        }
    }


    /* -------------------------- help method ------------------------ */

    public static boolean nodeHasGroupBy(PlanNode node) {
        return (node.getSumFuncs().size() > 0 || node.getGroupBys().size() > 0);
    }

    @Override
    protected String visitPushDownNameSel(Item item) {
        String orgPushDownName = item.getItemName();
        if (item.type().equals(Item.ItemType.FIELD_ITEM)) {
            orgPushDownName = "`" + item.getTableName() + "`.`" + orgPushDownName + "`";
        }
        String pushAlias = null;
        if (pushNameMap.containsKey(orgPushDownName)) {
            // duplicate column
            item.setPushDownName(pushNameMap.get(orgPushDownName));
            return null;
        }
        if (item.type().equals(Item.ItemType.SUM_FUNC_ITEM)) {
            // generate alias for aggregate function,it must contain the real name
            String aggName = ((ItemSum) item).funcName().toUpperCase();
            pushAlias = getMadeAggAlias(aggName) + getRandomAliasName();
        } else if (item.getAlias() != null) {
            pushAlias = item.getAlias();
            if (pushAlias.startsWith(Item.FNAF))
                pushAlias = getRandomAliasName();
        } else if (orgPushDownName.length() > MAX_COL_LENGTH) {
            pushAlias = getRandomAliasName();
        } else if (isTopQuery && !item.type().equals(Item.ItemType.FIELD_ITEM)) {
            pushAlias = getRandomAliasName();
        }
        if (pushAlias == null) {
            if (item.type().equals(Item.ItemType.FIELD_ITEM)) {
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
