/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor;

import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.subquery.ItemAllAnySubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemExistsSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemInSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemScalarSubQuery;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.util.StringUtil;


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
        if (query.isWithSubQuery() && !parentIsQuery && !isTopQuery) {
            sqlBuilder.append(" ( ");
        }
        if (query.isWithSubQuery() || isTopQuery) {
            buildSelect(query);

            if (query.getTableName() == null)
                return;
            sqlBuilder.append(" from ");
        }
        buildTableName(query, sqlBuilder);
        if (query.isWithSubQuery() || isTopQuery) {
            buildWhere(query);
            buildGroupBy(query);
            buildHaving(query);
            buildOrderBy(query);
            buildLimit(query);
        } else {
            whereFilter = query.getWhereFilter();
        }

        if (query.isWithSubQuery() && !parentIsQuery && !isTopQuery) {
            sqlBuilder.append(" ) ");
            if (query.getAlias() != null) {
                sqlBuilder.append(" ").append(query.getAlias()).append(" ");
            }
        }
        visited = true;
    }

    protected void visit(NoNameNode query) {
        if (!isTopQuery) {
            sqlBuilder.append(" ( ");
        }
        buildSelect(query);
        if (!isTopQuery) {
            sqlBuilder.append(" ) ");
            if (query.getAlias() != null) {
                sqlBuilder.append(" ").append(query.getAlias()).append(" ");
            }
        }
    }

    protected void visit(QueryNode query) {
        if (query.isWithSubQuery() && !isTopQuery) {
            sqlBuilder.append(" ( ");
        }
        if (query.isWithSubQuery() || isTopQuery) {
            buildSelect(query);
            sqlBuilder.append(" from ");
        }
        sqlBuilder.append('(');
        PlanNode child = query.getChild();
        MysqlVisitor childVisitor = new GlobalVisitor(child, true);
        childVisitor.visit();
        mapTableToSimple.putAll(childVisitor.getMapTableToSimple());
        sqlBuilder.append(childVisitor.getSql()).append(") ").append(query.getAlias());
        if (query.isWithSubQuery() || isTopQuery) {
            buildWhere(query);
            buildGroupBy(query);
            buildHaving(query);
            buildOrderBy(query);
            buildLimit(query);
        }

        if (query.isWithSubQuery() && !isTopQuery) {
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
            mapTableToSimple.putAll(childVisitor.getMapTableToSimple());
            sqlBuilder.append(childVisitor.getSql());
            if (child.getChildren().size() == 0) {
                sqlBuilder.append(")");
            }
        }

        if (merge.getOrderBys() != null && merge.getOrderBys().size() > 0) {
            sqlBuilder.append(" ORDER BY ");
            for (Order order : merge.getOrderBys()) {
                sqlBuilder.append(order.getItem().getItemName()).append(" ").append(order.getSortOrder()).append(",");
            }
            sqlBuilder.setLength(sqlBuilder.length() - 1);
        }

        if (merge.getLimitTo() != -1) {
            sqlBuilder.append(" LIMIT ");
            if (merge.getLimitFrom() != -1) {
                sqlBuilder.append(merge.getLimitFrom()).append(",");
            }
            sqlBuilder.append(merge.getLimitTo());
        }
    }

    protected void visit(JoinNode join) {
        if (!isTopQuery) {
            sqlBuilder.append(" ( ");
        }
        if (join.isWithSubQuery() || isTopQuery) {
            buildSelect(join);
            sqlBuilder.append(" from ");
        }

        PlanNode left = join.getLeftNode();
        MysqlVisitor leftVisitor = new GlobalVisitor(left, false);
        leftVisitor.visit();
        mapTableToSimple.putAll(leftVisitor.getMapTableToSimple());
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
        mapTableToSimple.putAll(rightVisitor.getMapTableToSimple());
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
            joinOnFilterStr.append(getItemName(filter));
        }

        if (join.getOtherJoinOnFilter() != null) {
            if (first) {
                sqlBuilder.append(" on ");
            } else {
                joinOnFilterStr.append(" and ");
            }
            joinOnFilterStr.append(join.getOtherJoinOnFilter());
        }
        sqlBuilder.append(joinOnFilterStr.toString());
        if (join.isWithSubQuery() || isTopQuery) {
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


    private void buildSelect(PlanNode query) {
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

    private void buildGroupBy(PlanNode query) {
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
                    pdName = "'" + StringUtil.trim(groupCol.toString(), '\'') + "'";
                if (pdName.isEmpty())
                    pdName = visitUnSelPushDownName(groupCol, true);
                sqlBuilder.append(pdName).append(" ").append(group.getSortOrder());
            }
        }
    }

    private void buildHaving(PlanNode query) {
        if (query.getHavingFilter() != null) {
            Item filter = query.getHavingFilter();
            String pdName = visitUnSelPushDownName(filter, true);
            sqlBuilder.append(" having ").append(pdName);
        }
    }

    private void buildOrderBy(PlanNode query) {
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
                    pdName = "'" + StringUtil.trim(orderByCol.toString(), '\'') + "'";
                if (pdName.isEmpty())
                    pdName = visitUnSelPushDownName(orderByCol, true);
                sqlBuilder.append(pdName).append(" ").append(order.getSortOrder());
            }
        }
    }

    private void buildLimit(PlanNode query) {
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
        String orgPushDownName;
        if (item.isWithSubQuery()) {
            orgPushDownName = buildSubQueryItem(item, false);
        } else {
            orgPushDownName = item.getItemName();
        }
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

    // pushDown's name of not in select list
    protected final String visitUnSelPushDownName(Item item, boolean canUseAlias) {
        if (item.isWithSubQuery()) {
            return buildSubQueryItem(item, canUseAlias);
        }
        String selName = getItemName(item);
        String nameInMap = pushNameMap.get(selName);
        if (nameInMap != null) {
            item.setPushDownName(nameInMap);
            if (canUseAlias && !(query.type() == PlanNodeType.JOIN && item.type().equals(ItemType.FIELD_ITEM))) {
                // join: select t1.id,t2.id from t1,t2 order by t1.id
                // try to use the origin group by,order by
                selName = nameInMap;
            }
        }
        return selName;
    }

    private String buildSubQueryItem(Item item, boolean canUseAlias) {
        if (!item.isWithSubQuery()) {
            return visitUnSelPushDownName(item, canUseAlias);
        }
        if (PlanUtil.isCmpFunc(item)) {
            return buildCmpSubQueryItem((ItemBoolFunc2) item, canUseAlias);
        } else if (item instanceof ItemInSubQuery) {
            ItemInSubQuery inSubItem = (ItemInSubQuery) item;
            StringBuilder builder = new StringBuilder();
            builder.append(visitUnSelPushDownName(inSubItem.getLeftOperand(), canUseAlias));
            if (inSubItem.isNeg()) {
                builder.append(" not ");
            }
            builder.append(" in ");
            PlanNode child = inSubItem.getPlanNode();
            MysqlVisitor childVisitor = new GlobalVisitor(child, true);
            childVisitor.visit();
            builder.append("(");
            builder.append(childVisitor.getSql());
            builder.append(")");
            mapTableToSimple.putAll(childVisitor.getMapTableToSimple());
            return builder.toString();
        } else if (item instanceof ItemExistsSubQuery) {
            ItemExistsSubQuery existsSubQuery = (ItemExistsSubQuery) item;
            StringBuilder builder = new StringBuilder();
            if (existsSubQuery.isNot()) {
                builder.append(" not ");
            }
            builder.append(" exists ");
            PlanNode child = existsSubQuery.getPlanNode();
            MysqlVisitor childVisitor = new GlobalVisitor(child, true);
            childVisitor.visit();
            builder.append("(");
            builder.append(childVisitor.getSql());
            builder.append(")");
            mapTableToSimple.putAll(childVisitor.getMapTableToSimple());
            return builder.toString();
        } else if (item instanceof ItemCondAnd || item instanceof ItemCondOr) {
            String cond;
            if (item instanceof ItemCondAnd) {
                cond = " and ";
            } else {
                cond = " or ";
            }
            StringBuilder builder = new StringBuilder();
            for (int index = 0; index < item.getArgCount(); index++) {
                if (index > 0) {
                    builder.append(cond);
                }
                builder.append(buildSubQueryItem(item.arguments().get(index), canUseAlias));
            }
            return builder.toString();
        } else if (item instanceof ItemScalarSubQuery) {
            return buildScalarSubQuery((ItemScalarSubQuery) item);
        }
        return visitUnSelPushDownName(item, canUseAlias);
    }

    private String buildScalarSubQuery(ItemScalarSubQuery item) {
        PlanNode child = item.getPlanNode();
        MysqlVisitor childVisitor = new GlobalVisitor(child, true);
        childVisitor.visit();
        mapTableToSimple.putAll(childVisitor.getMapTableToSimple());
        return "(" + childVisitor.getSql() + ")";
    }

    private String buildCmpSubQueryItem(ItemBoolFunc2 item, boolean canUseAlias) {
        return buildCmpArgSubQueryItem(item.arguments().get(0), canUseAlias) +
                item.funcName() +
                buildCmpArgSubQueryItem(item.arguments().get(1), canUseAlias);
    }

    private String buildCmpArgSubQueryItem(Item arg, boolean canUseAlias) {
        if (arg.type().equals(ItemType.SUBSELECT_ITEM)) {
            if (arg instanceof ItemScalarSubQuery) {
                return buildScalarSubQuery((ItemScalarSubQuery) arg);
            } else if (arg instanceof ItemAllAnySubQuery) {
                StringBuilder builder = new StringBuilder();
                ItemAllAnySubQuery allAnySubItem = (ItemAllAnySubQuery) arg;
                if (allAnySubItem.isAll()) {
                    builder.append(" all ");
                } else {
                    builder.append(" any ");
                }
                PlanNode child = allAnySubItem.getPlanNode();
                MysqlVisitor childVisitor = new GlobalVisitor(child, true);
                childVisitor.visit();
                builder.append("(");
                builder.append(childVisitor.getSql());
                builder.append(")");
                mapTableToSimple.putAll(childVisitor.getMapTableToSimple());
                return builder.toString();
            }
        }
        return buildSubQueryItem(arg, canUseAlias);
    }

}
