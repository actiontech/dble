/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.route.parser.druid.sql.visitor.ActionSQLEvalVisitorUtils;
import com.actiontech.dble.route.util.RouterUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ServerSchemaStatVisitor
 *
 * @author wang.dw
 */
public class ServerSchemaStatVisitor extends MySqlSchemaStatVisitor {
    private String notSupportMsg = null;
    private boolean hasOrCondition = false;
    private List<WhereUnit> whereUnits = new CopyOnWriteArrayList<>();
    private List<WhereUnit> storedWhereUnits = new CopyOnWriteArrayList<>();
    private boolean notInWhere = false;
    private List<SQLSelect> subQueryList = new ArrayList<>();

    private void reset() {
        this.conditions.clear();
        this.whereUnits.clear();
        this.hasOrCondition = false;
    }

    public List<SQLSelect> getSubQueryList() {
        return subQueryList;
    }

    public String getNotSupportMsg() {
        return notSupportMsg;
    }

    public boolean hasOrCondition() {
        return hasOrCondition;
    }

    @Override
    public boolean visit(SQLInSubQueryExpr x) {
        super.visit(x);
        subQueryList.add(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLQueryExpr x) {
        super.visit(x);
        subQueryList.add(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLListExpr x) {
        super.visit(x);
        notSupportMsg = "Row Subqueries is not supported";
        return true;
    }

    @Override
    public boolean visit(SQLExistsExpr x) {
        super.visit(x);
        subQueryList.add(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLAllExpr x) {
        super.visit(x);
        subQueryList.add(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLSomeExpr x) {
        super.visit(x);
        subQueryList.add(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLAnyExpr x) {
        super.visit(x);
        subQueryList.add(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        //need to protect parser SQLSelectItem, or SQLBinaryOpExpr may add to whereUnit
        // eg:id =1 will add to whereUnit
        notInWhere = true;
        x.getExpr().accept(this);
        notInWhere = false;

        //alias for select item is useless
        //        String alias = x.getAlias();
        //
        //        Map<String, String> aliasMap = this.getAliasMap();
        //        if (alias != null && (!alias.isEmpty()) && aliasMap != null) {
        //            if (x.getExpr() instanceof SQLName) {
        //                boolean isSelf = false;
        //                String itemName = x.getExpr().toString();
        //                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
        //                    isSelf = StringUtil.equalsIgnoreCase(alias, itemName);
        //                } else {
        //                    isSelf = StringUtil.equals(alias, itemName);
        //                }
        //                if (!isSelf) {
        //                    putAliasToMap(aliasMap, alias, x.getExpr().toString());
        //                }
        //            } else {
        //                putAliasToMap(aliasMap, alias, null);
        //            }
        //        }
        return false;
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        setAliasMap();
        return true;
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        if (x.isNot()) {
            return true;
        }
        String begin;
        if (x.beginExpr instanceof SQLCharExpr) {
            begin = (String) ((SQLCharExpr) x.beginExpr).getValue();
        } else {
            Object value = ActionSQLEvalVisitorUtils.eval(this.getDbType(), x.beginExpr, this.getParameters(), false);
            if (value != null) {
                begin = value.toString();
            } else {
                begin = x.beginExpr.toString();
            }

        }
        String end;
        if (x.endExpr instanceof SQLCharExpr) {
            end = (String) ((SQLCharExpr) x.endExpr).getValue();
        } else {
            Object value = ActionSQLEvalVisitorUtils.eval(this.getDbType(), x.endExpr, this.getParameters(), false);
            if (value != null) {
                end = value.toString();
            } else {
                end = x.endExpr.toString();
            }
        }
        Column column = getColumn(x);
        if (column == null) {
            return true;
        }

        Condition condition = null;
        for (Condition item : this.getConditions()) {
            if (item.getColumn().equals(column) && item.getOperator().equals("between")) {
                condition = item;
                break;
            }
        }

        if (condition == null) {
            condition = new Condition();
            condition.setColumn(column);
            condition.setOperator("between");
            this.conditions.add(condition);
        }


        condition.getValues().add(begin);
        condition.getValues().add(end);


        return true;
    }

    @Override
    public boolean visit(SQLBinaryOpExpr x) {
        if (isUnaryParentEffect(x)) return true;
        x.getLeft().setParent(x);
        x.getRight().setParent(x);

        switch (x.getOperator()) {
            case Equality:
            case LessThanOrEqualOrGreaterThan:
            case Is:
            case IsNot:
                if (!notInWhere) {
                    handleCondition(x.getLeft(), x.getOperator().name, x.getRight());
                    handleCondition(x.getRight(), x.getOperator().name, x.getLeft());
                    handleRelationship(x.getLeft(), x.getOperator().name, x.getRight());
                }
                break;
            case BooleanOr:
                //remove always true
                if (!RouterUtil.isConditionAlwaysTrue(x) && !notInWhere) {
                    hasOrCondition = true;

                    WhereUnit whereUnit;
                    if (conditions.size() > 0) {
                        whereUnit = new WhereUnit();
                        whereUnit.setFinishedParse(true);
                        whereUnit.addOutConditions(getConditions());
                        WhereUnit innerWhereUnit = new WhereUnit(x);
                        whereUnit.addSubWhereUnit(innerWhereUnit);
                    } else {
                        whereUnit = new WhereUnit(x);
                        whereUnit.addOutConditions(getConditions());
                    }
                    whereUnits.add(whereUnit);
                }
                return false;
            case Like:
            case NotLike:
            case NotEqual:
            case GreaterThan:
            case GreaterThanOrEqual:
            case LessThan:
            case LessThanOrEqual:
            default:
                break;
        }
        return true;
    }

    @Override
    public boolean visit(MySqlInsertStatement x) {
        SQLName sqlName = x.getTableName();
        if (sqlName != null) {
            setCurrentTable(sqlName.toString());
        }
        return false;
    }

    // DUAL
    @Override
    public boolean visit(MySqlDeleteStatement x) {
        setAliasMap();

        accept(x.getFrom());
        accept(x.getUsing());
        x.getTableSource().accept(this);

        if (x.getTableSource() instanceof SQLExprTableSource) {
            SQLName tableName = (SQLName) ((SQLExprTableSource) x.getTableSource()).getExpr();
            String ident = tableName.toString();
            setCurrentTable(x, ident);
        }

        accept(x.getWhere());

        accept(x.getOrderBy());
        accept(x.getLimit());

        return false;
    }

    @Override
    public boolean visit(SQLUpdateStatement x) {
        setAliasMap();

        SQLName identName = x.getTableName();
        if (identName != null) {
            String ident = identName.toString();
            setCurrentTable(ident);

            Map<String, String> aliasMap = getAliasMap();
            aliasMap.put(ident, ident);

            String alias = x.getTableSource().getAlias();
            if (alias != null) {
                aliasMap.put(alias, ident);
            }
        } else {
            x.getTableSource().accept(this);
        }

        accept(x.getItems());
        accept(x.getWhere());

        return false;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        if (this.isSimpleExprTableSource(x)) {
            String ident = x.getExpr().toString();
            if (this.variants.containsKey(ident)) {
                return false;
            }

            if (this.containsSubQuery(ident)) {
                return false;
            }

            Map<String, String> aliasMap = this.getAliasMap();
            if (aliasMap != null) {
                String alias = x.getAlias();
                if (alias != null && !aliasMap.containsKey(alias)) {
                    putAliasToMap(aliasMap, alias, ident);
                }

                if (!aliasMap.containsKey(ident)) {
                    putAliasToMap(aliasMap, ident, ident);
                }
            }
        } else {
            this.accept(x.getExpr());
        }
        return false;
    }



    @Override
    public boolean visit(SQLSelect x) {
        if (x.getOrderBy() != null) {
            x.getOrderBy().setParent(x);
        }

        this.accept(x.getWithSubQuery());
        this.accept(x.getQuery());
        this.accept(x.getOrderBy());
        return false;
    }

    @Override
    public boolean visit(SQLSelectQueryBlock x) {
        if (x.getFrom() == null) {
            return false;
        } else {
            if (x.getFrom() instanceof SQLSubqueryTableSource) {
                x.getFrom().accept(this);
                return false;
            } else {
                if (x.getFrom() instanceof SQLExprTableSource) {
                    SQLExprTableSource tableSource = (SQLExprTableSource) x.getFrom();
                    if (tableSource.getExpr() instanceof SQLName) {
                        String ident = tableSource.getExpr().toString();
                        this.setCurrentTable(ident);
                    }
                }
                if (x.getFrom() != null) {
                    x.getFrom().accept(this);
                }

                if (x.getWhere() != null) {
                    x.getWhere().setParent(x);
                }

                return true;
            }
        }
    }

    @Override
    public void endVisit(SQLSelectQueryBlock x) {
    }

    @Override
    public void endVisit(SQLSelect x) {
    }

    @Override
    public void endVisit(MySqlDeleteStatement x) {
    }

    @Override
    protected Column getColumn(SQLExpr expr) {
        Map<String, String> aliasMap = getAliasMap();
        if (aliasMap == null) {
            return null;
        }

        if (expr instanceof SQLPropertyExpr) {
            return getColumnByExpr((SQLPropertyExpr) expr, aliasMap);
        }

        if (expr instanceof SQLIdentifierExpr) {
            return getColumnByExpr((SQLIdentifierExpr) expr, aliasMap);
        }

        if (expr instanceof SQLBetweenExpr) {
            return getColumnByExpr((SQLBetweenExpr) expr, aliasMap);
        }
        return null;
    }

    @Override
    protected void handleCondition(SQLExpr expr, String operator, SQLExpr... valueExprs) {
        if (expr instanceof SQLCastExpr) {
            expr = ((SQLCastExpr) expr).getExpr();
        }

        Column column = this.getColumn(expr);
        if (column != null) {
            Condition condition = null;
            Iterator var6 = this.getConditions().iterator();

            while (var6.hasNext()) {
                Condition item = (Condition) var6.next();
                if (item.getColumn().equals(column) && item.getOperator().equals(operator)) {
                    condition = item;
                    break;
                }
            }

            if (condition == null) {
                condition = new Condition();
                condition.setColumn(column);
                condition.setOperator(operator);
                this.conditions.add(condition);
            }

            SQLExpr[] var12 = valueExprs;
            int var13 = valueExprs.length;

            for (int var8 = 0; var8 < var13; ++var8) {
                SQLExpr item = var12[var8];
                Column valueColumn = this.getColumn(item);
                if (valueColumn == null) {
                    Object value = ActionSQLEvalVisitorUtils.eval(this.getDbType(), item, this.getParameters(), false);
                    condition.getValues().add(value);
                }
            }

        }
    }

    private Column getColumnByExpr(SQLBetweenExpr betweenExpr, Map<String, String> aliasMap) {
        if (betweenExpr.getTestExpr() != null) {
            String tableName = null;
            String column = null;
            if (betweenExpr.getTestExpr() instanceof SQLPropertyExpr) { //field has alias
                tableName = ((SQLIdentifierExpr) ((SQLPropertyExpr) betweenExpr.getTestExpr()).getOwner()).getName();
                column = ((SQLPropertyExpr) betweenExpr.getTestExpr()).getName();
                SQLObject query = this.subQueryMap.get(tableName);
                if (query == null) {
                    if (aliasMap.containsKey(tableName)) {
                        tableName = aliasMap.get(tableName);
                    }
                    return new Column(tableName, column);
                }
                return handleSubQueryColumn(tableName, column);
            } else if (betweenExpr.getTestExpr() instanceof SQLIdentifierExpr) {
                column = ((SQLIdentifierExpr) betweenExpr.getTestExpr()).getName();
                tableName = getOwnerTableName(betweenExpr, column);
            }
            String table = tableName;
            if (aliasMap.containsKey(table)) {
                table = aliasMap.get(table);
            }

            if (variants.containsKey(table)) {
                return null;
            }

            if (table != null && !"".equals(table)) {
                return new Column(table, column);
            }
        }
        return null;
    }

    private Column getColumnByExpr(SQLIdentifierExpr expr, Map<String, String> aliasMap) {
        Column attrColumn = (Column) expr.getAttribute(ATTR_COLUMN);
        if (attrColumn != null) {
            return attrColumn;
        }

        String column = expr.getName();
        String table = getCurrentTable();
        if (table != null && aliasMap.containsKey(table)) {
            table = aliasMap.get(table);
            if (table == null) {
                return null;
            }
        }

        if (table != null) {
            return new Column(table, column);
        }

        if (variants.containsKey(column)) {
            return null;
        }

        return new Column("UNKNOWN", column);
    }

    private Column getColumnByExpr(SQLPropertyExpr expr, Map<String, String> aliasMap) {
        SQLExpr owner = expr.getOwner();
        String column = expr.getName();

        if (owner instanceof SQLIdentifierExpr || owner instanceof SQLPropertyExpr) {
            String tableName;
            if (owner instanceof SQLPropertyExpr) {
                tableName = ((SQLPropertyExpr) owner).getName();
            } else {
                tableName = ((SQLIdentifierExpr) owner).getName();
            }
            String table = tableName;
            if (aliasMap.containsKey(table)) {
                table = aliasMap.get(table);
            }

            if (variants.containsKey(table)) {
                return null;
            }

            if (table != null) {
                return new Column(table, column);
            }

            return handleSubQueryColumn(tableName, column);
        }

        return null;
    }

    /**
     * get table name of field in between expr
     *
     * @param betweenExpr
     * @param column
     * @return
     */
    private String getOwnerTableName(SQLBetweenExpr betweenExpr, String column) {
        if (aliasMap.size() == 1) { //only has 1 table
            return aliasMap.keySet().iterator().next();
        } else if (aliasMap.size() == 0) { //no table
            return "";
        } else { // multi tables
            for (Column col : columns.keySet()) {
                if (col.getName().equals(column)) {
                    return col.getTable();
                }
            }

            //parser from parent
            SQLObject parent = betweenExpr.getParent();
            if (parent instanceof SQLBinaryOpExpr) {
                parent = parent.getParent();
            }

            if (parent instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock select = (MySqlSelectQueryBlock) parent;
                if (select.getFrom() instanceof SQLJoinTableSource) {
                    SQLJoinTableSource joinTableSource = (SQLJoinTableSource) select.getFrom();
                    //FIXME :left as driven table
                    return joinTableSource.getLeft().toString();
                } else if (select.getFrom() instanceof SQLExprTableSource) {
                    return select.getFrom().toString();
                }
            } else if (parent instanceof SQLUpdateStatement) {
                SQLUpdateStatement update = (SQLUpdateStatement) parent;
                return update.getTableName().getSimpleName();
            } else if (parent instanceof SQLDeleteStatement) {
                SQLDeleteStatement delete = (SQLDeleteStatement) parent;
                return delete.getTableName().getSimpleName();
            }
        }
        return "";
    }

    /**
     * splitConditions
     */
    public List<List<Condition>> splitConditions() {
        //split according to or expr
        for (WhereUnit whereUnit : whereUnits) {
            splitUntilNoOr(whereUnit);
        }

        this.storedWhereUnits.addAll(whereUnits);

        loopFindSubWhereUnit(whereUnits);

        for (WhereUnit whereUnit : storedWhereUnits) {
            this.getConditionsFromWhereUnit(whereUnit);
        }

        return mergedConditions();
    }

    /**
     * FIND WhereUnit
     *
     * @param whereUnitList
     */
    private void loopFindSubWhereUnit(List<WhereUnit> whereUnitList) {
        List<WhereUnit> subWhereUnits = new ArrayList<>();
        for (WhereUnit whereUnit : whereUnitList) {
            if (whereUnit.getSplitedExprList().size() > 0) {
                List<SQLExpr> removeSplitedList = new ArrayList<>();
                for (SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
                    reset();
                    if (isExprHasOr(sqlExpr)) {
                        removeSplitedList.add(sqlExpr);
                        WhereUnit subWhereUnit = this.whereUnits.get(0);
                        splitUntilNoOr(subWhereUnit);
                        whereUnit.addSubWhereUnit(subWhereUnit);
                        subWhereUnits.add(subWhereUnit);
                    } else {
                        this.conditions.clear();
                    }
                }
                if (removeSplitedList.size() > 0) {
                    whereUnit.getSplitedExprList().removeAll(removeSplitedList);
                }
            }
            subWhereUnits.addAll(whereUnit.getSubWhereUnit());
        }
        if (subWhereUnits.size() > 0) {
            loopFindSubWhereUnit(subWhereUnits);
        }
    }

    private boolean isExprHasOr(SQLExpr expr) {
        expr.accept(this);
        return hasOrCondition;
    }

    private List<List<Condition>> mergedConditions() {
        if (storedWhereUnits.size() == 0) {
            return new ArrayList<>();
        }
        for (WhereUnit whereUnit : storedWhereUnits) {
            mergeOneWhereUnit(whereUnit);
        }
        return getMergedConditionList(storedWhereUnits);

    }

    /**
     * mergeOneWhereUnit
     *
     * @param whereUnit
     */
    private void mergeOneWhereUnit(WhereUnit whereUnit) {
        if (whereUnit.getSubWhereUnit().size() > 0) {
            for (WhereUnit sub : whereUnit.getSubWhereUnit()) {
                mergeOneWhereUnit(sub);
            }

            if (whereUnit.getSubWhereUnit().size() > 1) {
                List<List<Condition>> mergedConditionList = getMergedConditionList(whereUnit.getSubWhereUnit());
                if (whereUnit.getOutConditions().size() > 0) {
                    for (List<Condition> aMergedConditionList : mergedConditionList) {
                        aMergedConditionList.addAll(whereUnit.getOutConditions());
                    }
                }
                whereUnit.setConditionList(mergedConditionList);
            } else if (whereUnit.getSubWhereUnit().size() == 1) {
                if (whereUnit.getOutConditions().size() > 0 && whereUnit.getSubWhereUnit().get(0).getConditionList().size() > 0) {
                    for (int i = 0; i < whereUnit.getSubWhereUnit().get(0).getConditionList().size(); i++) {
                        whereUnit.getSubWhereUnit().get(0).getConditionList().get(i).addAll(whereUnit.getOutConditions());
                    }
                }
                whereUnit.getConditionList().addAll(whereUnit.getSubWhereUnit().get(0).getConditionList());
            }
        } else {
            //do nothing
        }
    }

    /**
     * merge WhereUnit's condition
     *
     * @return
     */
    private List<List<Condition>> getMergedConditionList(List<WhereUnit> whereUnitList) {
        List<List<Condition>> mergedConditionList = new ArrayList<>();
        if (whereUnitList.size() == 0) {
            return mergedConditionList;
        }
        mergedConditionList.addAll(whereUnitList.get(0).getConditionList());

        for (int i = 1; i < whereUnitList.size(); i++) {
            mergedConditionList = merge(mergedConditionList, whereUnitList.get(i).getConditionList());
        }
        return mergedConditionList;
    }

    /**
     * Merge 2 list
     *
     * @param list1
     * @param list2
     * @return
     */
    private List<List<Condition>> merge(List<List<Condition>> list1, List<List<Condition>> list2) {
        if (list1.size() == 0) {
            return list2;
        } else if (list2.size() == 0) {
            return list1;
        }

        List<List<Condition>> retList = new ArrayList<>();
        for (List<Condition> aList1 : list1) {
            for (List<Condition> aList2 : list2) {
                List<Condition> listTmp = new ArrayList<>();
                listTmp.addAll(aList1);
                listTmp.addAll(aList2);
                retList.add(listTmp);
            }
        }
        return retList;
    }

    private void getConditionsFromWhereUnit(WhereUnit whereUnit) {
        List<List<Condition>> retList = new ArrayList<>();
        List<Condition> outSideCondition = new ArrayList<>();
        outSideCondition.addAll(conditions);
        this.conditions.clear();
        for (SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
            sqlExpr.accept(this);
            List<Condition> conditions = new ArrayList<>();
            conditions.addAll(getConditions());
            conditions.addAll(outSideCondition);
            retList.add(conditions);
            this.conditions.clear();
        }
        whereUnit.setConditionList(retList);

        for (WhereUnit subWhere : whereUnit.getSubWhereUnit()) {
            getConditionsFromWhereUnit(subWhere);
        }
    }

    /**
     * splitUntilNoOr
     *
     * @param whereUnit
     */
    private void splitUntilNoOr(WhereUnit whereUnit) {
        if (whereUnit.isFinishedParse()) {
            if (whereUnit.getSubWhereUnit().size() > 0) {
                for (int i = 0; i < whereUnit.getSubWhereUnit().size(); i++) {
                    splitUntilNoOr(whereUnit.getSubWhereUnit().get(i));
                }
            }
        } else {
            SQLBinaryOpExpr expr = whereUnit.getCanSplitExpr();
            if (expr.getOperator() == SQLBinaryOperator.BooleanOr) {
                addExprIfNotFalse(whereUnit, expr.getRight());
                if (expr.getLeft() instanceof SQLBinaryOpExpr) {
                    whereUnit.setCanSplitExpr((SQLBinaryOpExpr) expr.getLeft());
                    splitUntilNoOr(whereUnit);
                } else {
                    addExprIfNotFalse(whereUnit, expr.getLeft());
                }
            } else {
                addExprIfNotFalse(whereUnit, expr);
                whereUnit.setFinishedParse(true);
            }
        }
    }

    private void addExprIfNotFalse(WhereUnit whereUnit, SQLExpr expr) {
        if (!RouterUtil.isConditionAlwaysFalse(expr)) {
            whereUnit.addSplitedExpr(expr);
        }
    }

    private boolean isUnaryParentEffect(SQLBinaryOpExpr x) {
        if (x.getParent() instanceof SQLUnaryExpr) {
            SQLUnaryExpr parent = (SQLUnaryExpr) x.getParent();
            switch (parent.getOperator()) {
                case Not:
                case NOT:
                case Compl:
                    return true;
                default:
                    break;
            }
        }
        return false;
    }

    private void putAliasToMap(Map<String, String> aliasMap, String name, String value) {
        if (aliasMap != null && name != null) {
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            aliasMap.put(name, value);
        }
    }

}
