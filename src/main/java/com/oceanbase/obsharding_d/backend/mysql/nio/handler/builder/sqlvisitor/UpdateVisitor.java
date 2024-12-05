/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.sqlvisitor;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemBasicConstant;
import com.oceanbase.obsharding_d.plan.common.item.ItemNull;
import com.oceanbase.obsharding_d.plan.common.item.ItemString;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.ItemBoolFunc2;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncIn;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncNe;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondAnd;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemCondOr;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.logic.ItemFuncNot;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;
import com.oceanbase.obsharding_d.plan.node.ModifyNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl.subquery.UpdateSubQueryHandler.NEED_REPLACE;


public class UpdateVisitor extends MysqlVisitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateVisitor.class);
    private final List<Item> valueItemList;
    private final List<Item> fieldList;
    protected boolean isExplain;

    public UpdateVisitor(PlanNode update, boolean isTopQuery, List<Item> valueItemList, List<Item> fieldList, boolean isExplain) {
        super(update, isTopQuery);
        this.valueItemList = valueItemList;
        this.fieldList = fieldList;
        this.isExplain = isExplain;
    }

    public void visit() {
        if (!visited) {
            replaceableSqlBuilder.clear();
            sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
            // if visited,push down visitor need just replace the name
            PlanNode.PlanNodeType i = query.type();
            if (i == PlanNode.PlanNodeType.MODIFY) {
                visit((ModifyNode) query);

            } else {
                throw new RuntimeException("not implement yet!");
            }
            visited = true;
        }
    }

    protected void visit(ModifyNode update) {
        sqlBuilder.append("update");

        //from
        update.getReferedTableNodes().stream()
                .forEach(tableNode -> {
                    if (update.getSetItemList().stream()
                            .anyMatch(setItem -> !StringUtil.isEmpty(setItem.arguments().get(0).getTableName()) && (setItem.arguments().get(0).getTableName().equals(tableNode.getAlias()) || setItem.arguments().get(0).getTableName().equals(tableNode.getTableName())))) {
                        buildTableName(tableNode, sqlBuilder);
                    }
                });

        //set
        sqlBuilder.append(" set ");
        String tableName = null;
        StringBuilder setBuilder = new StringBuilder();
        for (ItemFuncEqual itemFuncEqual : update.getSetItemList()) {
            Item setItem = itemFuncEqual.arguments().get(0);
            tableName = setItem.getTableName();
            if (StringUtil.isEmpty(tableName)) {
                setBuilder.append("`" + setItem.getItemName() + "`");
            } else {
                setBuilder.append("`" + setItem.getTableName() + "`.`" + setItem.getItemName() + "`");
            }
            setBuilder.append(" = ");
            Item valueItem = itemFuncEqual.arguments().get(1);
            String sqlStr = buildSQLStr(valueItem, tableName);
            setBuilder.append(sqlStr);
            setBuilder.append(",");
        }
        setBuilder.deleteCharAt(setBuilder.length() - 1);
        sqlBuilder.append(setBuilder);

        //where
        Item whereFilter = update.getWhereFilter();
        if (whereFilter != null) {
            Item item = rebuildUpdateItem(whereFilter, tableName);
            String selName = getUpdateItemName(item);
            sqlBuilder.append(" where ").append(selName);
        }
    }

    private String buildSQLStr(Item valueItem, String tableName) {
        if (valueItem instanceof ItemBasicConstant) {
            return valueItem.toString();
        } else if (!StringUtil.equalsIgnoreCase(tableName, valueItem.getTableName()) && isExplain) {
            return new ItemString(NEED_REPLACE, valueItem.getCharsetIndex()).toString();
        } else if (StringUtil.equalsIgnoreCase(tableName, valueItem.getTableName())) {
            return valueItem.toExpression().toString();
        } else {
            if (valueItemList.size() == 1) {
                //autoalias_scalar
                return valueItemList.get(0).toString();
            }
            int index = getItemIndex(valueItem);
            if (index < 0) {
                return valueItem.toExpression().toString();
            } else {
                return valueItemList.get(index).toString();
            }
        }
    }

    @Override
    protected String visitPushDownNameSel(Item o) {
        return null;
    }


    // try to trim sharding from field_item
    protected String getUpdateItemName(Item item) {
        if (item instanceof ItemCondOr) {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int index = 0; index < item.getArgCount(); index++) {
                if (index > 0) {
                    sb.append(" OR ");
                }
                sb.append(getUpdateItemName(item.arguments().get(index)));
            }
            sb.append(")");
            return sb.toString();
        } else if (item instanceof ItemCondAnd) {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int index = 0; index < item.getArgCount(); index++) {
                if (index > 0) {
                    sb.append(" AND ");
                }
                sb.append(getUpdateItemName(item.arguments().get(index)));
            }
            sb.append(")");
            return sb.toString();
        } else if (item instanceof ItemFuncNot) {
            return "(NOT " + getUpdateItemName(item.arguments().get(0)) + ")";
        } else if (item instanceof ItemBoolFunc2) {
            return getBoolFuncItemName(item);
        } else if (item.type().equals(Item.ItemType.FIELD_ITEM)) {
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
            sb.append(getUpdateItemName(a));
            if (((ItemFuncIn) item).isNegate()) {
                sb.append(" not ");
            }
            sb.append(" in (");
            for (int index = 1; index < item.arguments().size(); index++) {
                if (index > 1) {
                    sb.append(",");
                }
                sb.append(getUpdateItemName(item.arguments().get(index)));
            }
            sb.append(")");
            return sb.toString();
        } else {
            return item.getItemName();
        }
    }

    private String getBoolFuncItemName(Item item) {
        Item a = item.arguments().get(0);
        Item b = item.arguments().get(1);
        String left = getUpdateItemName(a), right = getUpdateItemName(b);
        if (a instanceof ItemNull && !(b instanceof ItemNull)) {
            left = getUpdateItemName(b);
            right = getUpdateItemName(a);
        }
        String operator = ((ItemBoolFunc2) item).funcName();
        if (a instanceof ItemNull || b instanceof ItemNull) {
            if (item instanceof ItemFuncEqual) {
                operator = SQLBinaryOperator.Is.getName();
            } else if (item instanceof ItemFuncNe) {
                operator = SQLBinaryOperator.IsNot.getName();
            }
        }
        return left + " " + operator + " " + right;
    }


    public Item rebuildUpdateItem(Item item, String tableName) {
        BoolPtr reBuild = new BoolPtr(false);
        if (PlanUtil.isCmpFunc(item)) {
            Item res1 = PlanUtil.rebuildBoolSubQuery(item, 0, reBuild, new BoolPtr(false), new BoolPtr(false));
            if (res1 != null) {
                return res1;
            }

            BoolPtr needExecuteNull = new BoolPtr(false);
            BoolPtr isAll = new BoolPtr(false);
            Item res2 = PlanUtil.rebuildBoolSubQuery(item, 1, reBuild, needExecuteNull, isAll);
            if (res2 != null) {
                return res2;
            }

            ItemFunc func = (ItemFunc) item;
            item.setWithSubQuery(false);
            Item itemTmp = item.cloneStruct();
            for (int index = 0; index < func.getArgCount(); index++) {
                Item arg = item.arguments().get(index);
                if (isExplain && !(arg instanceof ItemBasicConstant) && !StringUtil.equalsIgnoreCase(tableName, arg.getTableName())) {
                    itemTmp.arguments().set(index, new ItemString(NEED_REPLACE, itemTmp.getCharsetIndex()));
                    itemTmp.setItemName(null);
                } else if (arg instanceof ItemBasicConstant) {
                    continue;
                } else {
                    int fieldIndex = getItemIndex(arg);
                    if (fieldIndex >= 0) {
                        itemTmp.arguments().set(index, valueItemList.get(fieldIndex));
                        itemTmp.setItemName(null);
                    }
                }
            }
            return itemTmp;
        } else if (item instanceof ItemCondAnd || item instanceof ItemCondOr) {
            item.setWithSubQuery(false);
            Item itemTmp = item.cloneStruct();
            for (int index = 0; index < item.getArgCount(); index++) {
                Item rebuildItem = rebuildUpdateItem(item.arguments().get(index), tableName);
                itemTmp.arguments().set(index, rebuildItem);
                itemTmp.setItemName(null);
            }
            return itemTmp;
        }
        return item;
    }

    private int getItemIndex(Item valueItem) {
        if (fieldList.size() == 1 && valueItemList.size() == 1 && valueItem.getTableName().equals(fieldList.get(0).getTableName())) {
            return 0;
        }
        for (int i = 0; i < fieldList.size(); i++) {
            if (fieldList.get(i).equals(valueItem)) {
                return i;
            }
        }
        return -1;
    }

    @NotNull
    public RouteResultset buildRouteResultset() {
        this.visit();
        String sql = sqlBuilder.toString();
        LOGGER.debug("merge update——update sql:{}", sql);
        RouteResultset rrs = new RouteResultset(sql, ServerParse.UPDATE);
        rrs.setStatement(sql);
        rrs.setComplexSQL(true);
        return rrs;
    }
}
