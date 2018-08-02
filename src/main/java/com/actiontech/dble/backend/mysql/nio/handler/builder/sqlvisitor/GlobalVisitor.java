/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;


/**
 * sql generator,node is always global table,every  node must create a new global visitor object
 *
 * @author ActionTech
 * @CreateTime 2014/12/10
 */
public class GlobalVisitor extends MysqlVisitor {

    public GlobalVisitor(PlanNode globalQuery, boolean isTopQuery) {
        super(globalQuery, isTopQuery);
    }

    public void visit() {
        if (!visited) {
            replaceableSqlBuilder.clear();
            sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
            PlanNodeType i = query.type();
            if (i == PlanNodeType.TABLE) {
                visit((TableNode) query);

            } else if (i == PlanNodeType.JOIN) {
                visit((JoinNode) query);

            } else if (i == PlanNodeType.QUERY) {
                visit((QueryNode) query);

            } else if (i == PlanNodeType.MERGE) {
                visit((MergeNode) query);

            } else if (i == PlanNodeType.NONAME) {
                visit((NoNameNode) query);

            } else {
                throw new RuntimeException("not implement yet!");
            }
            visited = true;
        } else {
            // where just for table node without iteration
            buildWhere(query);
        }
    }

    protected void visit(TableNode query) {
        boolean parentIsQuery = query.getParent() != null && query.getParent().type() == PlanNodeType.QUERY;
        if (query.isSubQuery() && !parentIsQuery && !isTopQuery) {
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
            buildHaving(query);
            buildOrderBy(query);
            buildLimit(query);
        } else {
            whereFilter = query.getWhereFilter();
        }

        if (query.isSubQuery() && !parentIsQuery && !isTopQuery) {
            sqlBuilder.append(" ) ");
            if (query.getAlias() != null) {
                sqlBuilder.append(" ").append(query.getAlias()).append(" ");
            }
        }
        visited = true;
    }

    protected void visit(NoNameNode query) {
        //FIXME:如果在viewoptimizr时,将noname的where和select进行了修改,则需要改成和tablenode类似的做法
        if (!isTopQuery) {
            sqlBuilder.append(" ( ");
        }
        sqlBuilder.append(query.getSql());
        if (!isTopQuery) {
            sqlBuilder.append(" ) ");
            if (query.getAlias() != null) {
                sqlBuilder.append(" ").append(query.getAlias()).append(" ");
            }
        }
    }

    protected void visit(QueryNode query) {
        if (query.isSubQuery() && !isTopQuery) {
            sqlBuilder.append(" ( ");
        }
        if (query.isSubQuery() || isTopQuery) {
            buildSelect(query);
            sqlBuilder.append(" from ");
        }
        sqlBuilder.append('(');
        PlanNode child = query.getChild();
        MysqlVisitor childVisitor = new GlobalVisitor(child, true);
        childVisitor.visit();
        sqlBuilder.append(childVisitor.getSql()).append(") ").append(query.getAlias());
        if (query.isSubQuery() || isTopQuery) {
            buildWhere(query);
            buildGroupBy(query);
            buildHaving(query);
            buildOrderBy(query);
            buildLimit(query);
        }

        if (query.isSubQuery() && !isTopQuery) {
            sqlBuilder.append(" ) ");
            if (query.getAlias() != null) {
                sqlBuilder.append(" ").append(query.getAlias()).append(" ");
            }
        }
    }

    protected void visit(MergeNode merge) {
        boolean isUnion = merge.isUnion();
        boolean isFirst = true;
        for (PlanNode child : merge.getChildren()) {
            MysqlVisitor childVisitor = new GlobalVisitor(child, true);
            childVisitor.visit();
            if (isFirst) {
                isFirst = false;
            } else {
                sqlBuilder.append(isUnion ? " UNION " : " UNION ALL ");
            }
            if (child.getChildren().size() == 0) {
                sqlBuilder.append("(");
            }
            sqlBuilder.append(childVisitor.getSql());
            if (child.getChildren().size() == 0) {
                sqlBuilder.append(")");
            }
        }

        if (merge.getOrderBys() != null && merge.getOrderBys().size() > 0) {
            sqlBuilder.append(" ORDER BY ");
            for (Order order : merge.getOrderBys()) {
                sqlBuilder.append(order.getItem().getItemName() + " " + order.getSortOrder() + ",");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 1);
        }

        if (merge.getLimitTo() != 0) {
            sqlBuilder.append(" LIMIT ");
            if (merge.getLimitFrom() != 0) {
                sqlBuilder.append(merge.getLimitFrom() + ",");
            }
            sqlBuilder.append(merge.getLimitTo());
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
        sqlBuilder.append(leftVisitor.getSql());
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
        sqlBuilder.append(rightVisitor.getSql());
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
            if (!first) {
                joinOnFilterStr.append(" and ");
            }

            joinOnFilterStr.append(join.getOtherJoinOnFilter());
        }
        sqlBuilder.append(joinOnFilterStr.toString());
        if (join.isSubQuery() || isTopQuery) {
            buildWhere(join);
            buildGroupBy(join);
            buildHaving(join);
            buildOrderBy(join);
            buildLimit(join);
        } else {
            whereFilter = query.getWhereFilter();
        }
        if (!isTopQuery) {
            sqlBuilder.append(" ) ");
        }
    }


    protected void buildSelect(PlanNode query) {
        sqlBuilder.append("select ");
        boolean hasDistinct = query.isDistinct();
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Item selected : query.getColumnsSelected()) {
            if (first)
                first = false;
            else
                sb.append(",");
            String pdName = visitPushDownNameSel(selected);
            sb.append(pdName);
        }
        if (hasDistinct)
            sqlBuilder.append(" distinct ");
        sqlBuilder.append(sb);
    }

    protected void buildGroupBy(PlanNode query) {
        boolean first = true;
        if (query.getGroupBys() != null && query.getGroupBys().size() > 0) {
            sqlBuilder.append(" GROUP BY ");
            for (Order group : query.getGroupBys()) {
                if (first)
                    first = false;
                else
                    sqlBuilder.append(",");
                Item groupCol = group.getItem();
                String pdName = "";
                if (groupCol.basicConstItem())
                    pdName = "'" + groupCol.toString() + "'";
                if (pdName.isEmpty())
                    pdName = visitUnSelPushDownName(groupCol, true);
                sqlBuilder.append(pdName).append(" ").append(group.getSortOrder());
            }
        }
    }

    protected void buildHaving(PlanNode query) {
        if (query.getHavingFilter() != null) {
            Item filter = query.getHavingFilter();
            String pdName = visitUnSelPushDownName(filter, true);
            sqlBuilder.append(" having ").append(pdName);
        }
    }

    protected void buildOrderBy(PlanNode query) {
        boolean first = true;
        if (query.getOrderBys() != null && !query.getOrderBys().isEmpty()) {
            sqlBuilder.append(" order by ");
            for (Order order : query.getOrderBys()) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(",");
                }

                Item orderByCol = order.getItem();
                String pdName = "";
                if (orderByCol.basicConstItem())
                    pdName = "'" + orderByCol.toString() + "'";
                if (pdName.isEmpty())
                    pdName = visitUnSelPushDownName(orderByCol, true);
                sqlBuilder.append(pdName).append(" ").append(order.getSortOrder());
            }
        }
    }

    protected void buildLimit(PlanNode query) {
        long limitFrom = query.getLimitFrom();
        long limitTo = query.getLimitTo();
        if (limitFrom == -1 && limitTo == -1) {
            return;
        }
        sqlBuilder.append(" limit ");
        if (limitFrom > -1)
            sqlBuilder.append(limitFrom);
        if (limitTo != -1) {
            sqlBuilder.append(",").append(limitTo);
        }
    }

    /* -------------------------- help method ------------------------ */
    @Override
    protected String visitPushDownNameSel(Item item) {
        String orgPushDownName = item.getItemName();
        if (item.type().equals(ItemType.FIELD_ITEM)) {
            orgPushDownName = "`" + item.getTableName() + "`.`" + orgPushDownName + "`";
        }
        String pushAlias = null;
        if (item.getPushDownName() != null)
            //already set before
            pushAlias = item.getPushDownName();
        else if (item.getAlias() != null) {
            pushAlias = item.getAlias();
            if (pushAlias.startsWith(Item.FNAF))
                pushAlias = getRandomAliasName();
        } else if (orgPushDownName.length() > MAX_COL_LENGTH) { // define alias if>MAX_COL_LENGTH
            pushAlias = getRandomAliasName();
        }
        if (pushAlias == null) {
            if (item.type().equals(ItemType.FIELD_ITEM)) {
                pushNameMap.put(orgPushDownName, null);
            } else {
                item.setPushDownName(orgPushDownName);
                pushNameMap.put(orgPushDownName, orgPushDownName);
            }
            return orgPushDownName;
        } else {
            item.setPushDownName(pushAlias);
            pushNameMap.put(orgPushDownName, pushAlias);
            return orgPushDownName + " as `" + pushAlias + "`";
        }
    }
}
