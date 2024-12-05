/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.sqlvisitor;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.Item.ItemType;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.ItemBoolFunc2;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondAnd;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondOr;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemFuncNot;
import com.oceanbase.obsharding_d.plan.common.ptr.StringPtr;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode.PlanNodeType;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * execute the node which can push down,may be all global tables,and maybe contain global tables
 *
 * @author oceanbase
 * @CreateTime 2014/12/10
 */
public abstract class MysqlVisitor {
    // the max column size of mysql
    static final int MAX_COL_LENGTH = 255;
    // map :sel name->push name
    Map<String, String> pushNameMap = new HashMap<>();
    boolean isTopQuery = false;
    protected PlanNode query;
    private long randomIndex = 0L;

    /* is all function can be push down?if not,it need to calc by middle-ware */
    boolean existUnPushDownGroup = false;
    boolean visited = false;
    // -- start replaceable string builder
    ReplaceableStringBuilder replaceableSqlBuilder = new ReplaceableStringBuilder();
    // tmp sql
    StringBuilder sqlBuilder;
    StringPtr replaceableWhere = new StringPtr("");
    Item whereFilter = null;

    Map<String, String> mapTableToSimple = new HashMap<>();

    MysqlVisitor(PlanNode query, boolean isTopQuery) {
        this.query = query;
        this.isTopQuery = isTopQuery;
    }

    public ReplaceableStringBuilder getSql() {
        return replaceableSqlBuilder;
    }

    public Map<String, String> getMapTableToSimple() {
        return mapTableToSimple;
    }

    public abstract void visit();


    void buildTableName(TableNode tableNode, StringBuilder sb) {
        String tableName = "`" + tableNode.getPureName() + "`";
        String fullName = "`" + tableNode.getPureSchema() + "`." + tableName;
        mapTableToSimple.put(tableNode.getPureSchema() + "." + tableNode.getPureName(), tableNode.getPureName());
        mapTableToSimple.put(fullName, tableName);
        sb.append(" ").append(fullName);
        String alias = tableNode.getAlias();
        if (alias != null) {
            sb.append(" `").append(alias).append("`");
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
                ov.setPrettyFormat(false);
                ov.setShardingSupport(false);
                hint.accept(ov);
            }
        }
    }

    /* change where to replaceable */
    void buildWhere(PlanNode planNode) {
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

    void buildWhere(JoinNode planNode, MysqlVisitor leftVisitor, MysqlVisitor rightVisitor) {
        if (!visited)
            replaceableSqlBuilder.getCurrentElement().setRepString(replaceableWhere);
        StringBuilder whereBuilder = new StringBuilder();
        Item filter = planNode.getWhereFilter();
        if (filter != null) {
            String pdName = visitUnSelPushDownName(filter, false);
            whereBuilder.append(" where ").append(pdName);
        } else {
            whereBuilder.append(" where 1=1 ");
        }
        // is not left join
        if (leftVisitor.getWhereFilter() != null && !planNode.getLeftOuter()) {
            String pdName = visitUnSelPushDownName(leftVisitor.getWhereFilter(), false);
            whereBuilder.append(" and (");
            whereBuilder.append(pdName);
            whereBuilder.append(")");
        }
        // is not right join
        if (rightVisitor.getWhereFilter() != null && !planNode.getRightOuter()) {
            String pdName = visitUnSelPushDownName(rightVisitor.getWhereFilter(), false);
            whereBuilder.append(" and (");
            whereBuilder.append(pdName);
            whereBuilder.append(")");
        }
        // left join
        if (leftVisitor.getWhereFilter() != null && !planNode.getRightOuter() && planNode.getLeftOuter()) {
            String pdName = visitUnSelPushDownName(leftVisitor.getWhereFilter(), false);
            whereBuilder.append(" and (");
            whereBuilder.append(pdName);
            whereBuilder.append(")");
        }
        //right join
        if (rightVisitor.getWhereFilter() != null && !planNode.getLeftOuter() && planNode.getRightOuter()) {
            String pdName = visitUnSelPushDownName(rightVisitor.getWhereFilter(), false);
            whereBuilder.append(" and (");
            whereBuilder.append(pdName);
            whereBuilder.append(")");
        }
        replaceableWhere.set(whereBuilder.toString());
        // refresh sqlbuilder
        sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
    }

    // generate an alias for aggregate function
    public static String getMadeAggAlias(String aggFuncName) {
        return "_$" + aggFuncName + "$_";
    }

    String getRandomAliasName() {
        return "rpda_" + randomIndex++;
    }

    /**
     * generate push down
     */
    protected abstract String visitPushDownNameSel(Item o);

    // pushDown's name of not in select list
    protected String visitUnSelPushDownName(Item item, boolean canUseAlias) {
        if (item.isWithSubQuery()) {
            item = PlanUtil.rebuildSubQueryItem(item);
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

    // try to trim sharding from field_item
    protected String getItemName(Item item) {
        if (item instanceof ItemCondOr) {
            StringBuilder sb = new StringBuilder();
            sb.append(" ( ");
            for (int index = 0; index < item.getArgCount(); index++) {
                if (index > 0) {
                    sb.append(" OR ");
                }
                sb.append(getItemName(item.arguments().get(index)));
            }
            sb.append(")");
            return sb.toString();
        } else if (item instanceof ItemCondAnd) {
            StringBuilder sb = new StringBuilder();
            sb.append(" ( ");
            for (int index = 0; index < item.getArgCount(); index++) {
                if (index > 0) {
                    sb.append(" AND ");
                }
                sb.append(getItemName(item.arguments().get(index)));
            }
            sb.append(")");
            return sb.toString();
        } else if (item instanceof ItemFuncNot) {
            return " ( NOT " + getItemName(item.arguments().get(0)) + ")";
        } else if (item instanceof ItemBoolFunc2) {
            Item a = item.arguments().get(0);
            Item b = item.arguments().get(1);
            return getItemName(a) + " " + ((ItemBoolFunc2) item).funcName() + " " + getItemName(b);
        } else if (item.type().equals(ItemType.FIELD_ITEM)) {
            String tableName = "`" + item.getTableName() + "`.`" + item.getItemName() + "`";
            if (item.getDbName() == null) {
                return tableName;
            }
            if (item.getReferTables().size() == 0) {
                return tableName;
            }
            PlanNode tbNode = item.getReferTables().iterator().next();
            if (!(tbNode instanceof TableNode)) {
                return tableName;
            }
            if (!((TableNode) tbNode).getTableName().equals(item.getTableName())) {
                return tableName;
            }
            return "`" + item.getDbName() + "`." + tableName;
        } else if (item instanceof ItemFuncIn) {
            Item a = item.arguments().get(0);
            StringBuilder sb = new StringBuilder();
            sb.append(getItemName(a));
            if (((ItemFuncIn) item).isNegate()) {
                sb.append(" not ");
            }
            sb.append(" in (");
            for (int index = 1; index < item.arguments().size(); index++) {
                if (index > 1) {
                    sb.append(",");
                }
                sb.append(getItemName(item.arguments().get(index)));
            }
            sb.append(")");
            return sb.toString();
        } else {
            return item.getItemName();
        }
    }

    public Item getWhereFilter() {
        return whereFilter;
    }

    public boolean isTopQuery() {
        return isTopQuery;
    }

    public boolean isExistUnPushDownGroup() {
        return existUnPushDownGroup;
    }

    public StringBuilder getSqlBuilder() {
        return sqlBuilder;
    }
}
