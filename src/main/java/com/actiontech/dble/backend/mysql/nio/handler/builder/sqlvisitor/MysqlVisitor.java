/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor;

import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.subquery.ItemAllAnySubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemExistsSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemInSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemScalarSubQuery;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * execute the node which can push down,may be all global tables,and maybe contain global tables
 *
 * @author ActionTech
 * @CreateTime 2014/12/10
 */
public abstract class MysqlVisitor {
    // the max column size of mysql
    protected static final int MAX_COL_LENGTH = 255;
    // map :sel name->push name
    protected Map<String, String> pushNameMap = new HashMap<>();
    protected boolean isTopQuery = false;
    protected PlanNode query;
    protected long randomIndex = 0L;
    /* is all function can be push down?if not,it need to calc by middle-ware */
    protected boolean existUnPushDownGroup = false;
    protected boolean visited = false;
    // -- start replaceable string builder
    protected ReplaceableStringBuilder replaceableSqlBuilder = new ReplaceableStringBuilder();
    // tmp sql
    protected StringBuilder sqlBuilder;
    protected StringPtr replaceableWhere = new StringPtr("");
    protected Item whereFilter = null;
    public MysqlVisitor(PlanNode query, boolean isTopQuery) {
        this.query = query;
        this.isTopQuery = isTopQuery;
    }

    public ReplaceableStringBuilder getSql() {
        return replaceableSqlBuilder;
    }

    public abstract void visit();


    protected void buildTableName(TableNode tableNode, StringBuilder sb) {
        sb.append(" `").append(tableNode.getPureName()).append("`");
        String alias = tableNode.getAlias();
        if (tableNode.getSubAlias() != null) {
            if (alias != null) {
                sb.append(" `").append(alias).append("`");
            } else {
                sb.append(" `").append(tableNode.getSubAlias()).append("`");
            }
        }
        List<SQLHint> hintList = tableNode.getHintList();
        if (hintList != null && !hintList.isEmpty()) {
            sb.append(' ');
            boolean isFirst = true;
            for (SQLHint hint : hintList) {
                if (isFirst)
                    isFirst = false;
                else
                    sb.append(" ");
                MySqlOutputVisitor ov = new MySqlOutputVisitor(sb);
                hint.accept(ov);
            }
        }
    }

    /* change where to replaceable */
    protected void buildWhere(PlanNode planNode) {
        if (!visited)
            replaceableSqlBuilder.getCurrentElement().setRepString(replaceableWhere);
        StringBuilder whereBuilder = new StringBuilder();
        Item filter = planNode.getWhereFilter();
        if (filter != null) {
            String pdName = visitUnSelPushDownName(filter, false);
            whereBuilder.append(" where ").append(pdName);
        }

        replaceableWhere.set(whereBuilder.toString());
        // refresh sqlbuilder
        sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
    }

    // generate an alias for aggregate function
    public static String getMadeAggAlias(String aggFuncName) {
        return "_$" + aggFuncName + "$_";
    }

    protected String getRandomAliasName() {
        return "rpda_" + randomIndex++;
    }

    /**
     * generate push down
     */
    protected abstract String visitPushDownNameSel(Item o);

    private Item genBoolItem(boolean isTrue) {
        if (isTrue) {
            return new ItemFuncEqual(new ItemInt(1), new ItemInt(1));
        } else {
            return new ItemFuncEqual(new ItemInt(1), new ItemInt(0));
        }
    }

    private Item rebuildSubQueryItem(Item item) {
        if (PlanUtil.isCmpFunc(item)) {
            Item res1 = rebuildBoolSubQuery(item, 0);
            if (res1 != null) {
                return res1;
            }
            Item res2 = rebuildBoolSubQuery(item, 1);
            if (res2 != null) {
                return res2;
            }
        } else if (item instanceof ItemInSubQuery) {
            ItemInSubQuery inSubItem = (ItemInSubQuery) item;
            if (inSubItem.getValue().size() == 0) {
                return genBoolItem(inSubItem.isNeg());
            } else {
                List<Item> args = new ArrayList<>(inSubItem.getValue().size() + 1);
                args.add(inSubItem.getLeftOperand());
                args.addAll(inSubItem.getValue());
                return new ItemFuncIn(args, inSubItem.isNeg());
            }
        } else if (item instanceof ItemExistsSubQuery) {
            ItemExistsSubQuery existsSubQuery = (ItemExistsSubQuery) item;
            Item result = existsSubQuery.getValue();
            if (result == null) {
                return genBoolItem(existsSubQuery.isNot());
            } else {
                return genBoolItem(!existsSubQuery.isNot());
            }
        } else if (item instanceof ItemCondAnd || item instanceof ItemCondOr) {
            for (int index = 0; index < item.getArgCount(); index++) {
                Item rebuildItem = rebuildSubQueryItem(item.arguments().get(index));
                item.arguments().set(index, rebuildItem);
                item.setItemName(null);
            }
        }
        return item;
    }

    private Item rebuildBoolSubQuery(Item item, int index) {
        Item arg = item.arguments().get(index);
        if (arg.type().equals(ItemType.SUBSELECT_ITEM)) {
            if (arg instanceof ItemScalarSubQuery) {
                Item result = ((ItemScalarSubQuery) arg).getValue();
                if (result == null || result.getResultItem() == null) {
                    return new ItemFuncEqual(new ItemInt(1), new ItemInt(0));
                }
                item.arguments().set(index, result.getResultItem());
                item.setItemName(null);
            } else if (arg instanceof ItemAllAnySubQuery) {
                ItemAllAnySubQuery allAnySubItem = (ItemAllAnySubQuery) arg;
                if (allAnySubItem.getValue().size() == 0) {
                    return genBoolItem(allAnySubItem.isAll());
                } else if (allAnySubItem.getValue().size() == 1) {
                    Item value = allAnySubItem.getValue().get(0);
                    if (value == null) {
                        return new ItemFuncEqual(new ItemInt(1), new ItemInt(0));
                    }
                    item.arguments().set(index, value.getResultItem());
                    item.setItemName(null);
                } else {
                    return genBoolItem(!allAnySubItem.isAll());
                }
            }
        }
        return null;
    }

    // pushDown's name of not in select list
    public final String visitUnSelPushDownName(Item item, boolean canUseAlias) {
        if (item.isWithSubQuery()) {
            item = rebuildSubQueryItem(item);
        }
        String selName = item.getItemName();
        if (item.type().equals(ItemType.FIELD_ITEM)) {
            selName = "`" + item.getTableName() + "`.`" + selName + "`";
        }
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

    public Item getWhereFilter() {
        return whereFilter;
    }

}
