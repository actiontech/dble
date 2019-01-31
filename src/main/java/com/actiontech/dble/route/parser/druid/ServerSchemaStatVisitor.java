/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.DbleServer;
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
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.stat.TableStat.Relationship;

import java.util.*;
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
    private Map<String, String> aliasMap = new LinkedHashMap<>();
    private List<String> selectTableList = new ArrayList<>();
    private String currentTable;

    private void reset() {
        this.relationships.clear();
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
        aliasMap.clear();
        selectTableList.clear();
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
        } else if (x.beginExpr instanceof SQLNullExpr) {
            begin = x.beginExpr.toString();
        } else {
            Object value = SQLEvalVisitorUtils.eval(this.getDbType(), x.beginExpr, this.getParameters(), false);
            if (value != null) {
                begin = value.toString();
            } else {
                begin = x.beginExpr.toString();
            }

        }
        String end;
        if (x.endExpr instanceof SQLCharExpr) {
            end = (String) ((SQLCharExpr) x.endExpr).getValue();
        } else if (x.endExpr instanceof SQLNullExpr) {
            end = x.endExpr.toString();
        } else {
            Object value = SQLEvalVisitorUtils.eval(this.getDbType(), x.endExpr, this.getParameters(), false);
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
            condition = new Condition(column, "between");
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
                    whereUnit = new WhereUnit(x);
                    whereUnit.addOutConditions(getConditions());
                    whereUnit.addOutRelationships(getRelationships());
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
            currentTable = sqlName.toString();
        }
        return false;
    }

    // DUAL
    @Override
    public boolean visit(MySqlDeleteStatement x) {
        aliasMap.clear();
        accept(x.getFrom());
        accept(x.getUsing());
        x.getTableSource().accept(this);

        if (x.getTableSource() instanceof SQLExprTableSource) {
            SQLName tableName = (SQLName) ((SQLExprTableSource) x.getTableSource()).getExpr();
            currentTable = tableName.toString();
        }

        accept(x.getWhere());

        accept(x.getOrderBy());
        accept(x.getLimit());

        return false;
    }

    @Override
    public boolean visit(SQLUpdateStatement x) {
        aliasMap.clear();
        SQLName identName = x.getTableName();
        if (identName != null) {
            String ident = identName.toString();
            currentTable = ident;

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
            currentTable = ident;
            selectTableList.add(ident);
            String alias = x.getAlias();
            if (alias != null && !aliasMap.containsKey(alias)) {
                putAliasToMap(alias, ident);
            }

            if (!aliasMap.containsKey(ident)) {
                putAliasToMap(ident, ident);
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
        return true;
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
        if (aliasMap == null) {
            return null;
        }

        if (expr instanceof SQLPropertyExpr) {
            return getColumnByExpr((SQLPropertyExpr) expr);
        }

        if (expr instanceof SQLIdentifierExpr) {
            return getColumnByExpr((SQLIdentifierExpr) expr);
        }

        if (expr instanceof SQLBetweenExpr) {
            return getColumnByExpr((SQLBetweenExpr) expr);
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
                condition = new Condition(column, operator);
                this.conditions.add(condition);
            }

            SQLExpr[] var12 = valueExprs;
            int var13 = valueExprs.length;

            for (int var8 = 0; var8 < var13; ++var8) {
                SQLExpr item = var12[var8];
                Column valueColumn = this.getColumn(item);
                if (valueColumn == null) {
                    if (item instanceof SQLNullExpr) {
                        condition.getValues().add(item);
                    } else {
                        Object value = SQLEvalVisitorUtils.eval(this.getDbType(), item, this.getParameters(), false);
                        condition.getValues().add(value);
                    }
                }
            }

        }
    }

    private Column getColumnByExpr(SQLBetweenExpr betweenExpr) {
        if (betweenExpr.getTestExpr() != null) {
            String tableName = null;
            String column = null;
            if (betweenExpr.getTestExpr() instanceof SQLPropertyExpr) { //field has alias
                tableName = ((SQLIdentifierExpr) ((SQLPropertyExpr) betweenExpr.getTestExpr()).getOwner()).getName();
                column = ((SQLPropertyExpr) betweenExpr.getTestExpr()).getName();
                if (aliasMap.containsKey(tableName)) {
                    tableName = aliasMap.get(tableName);
                }
                return new Column(tableName, column);
            } else if (betweenExpr.getTestExpr() instanceof SQLIdentifierExpr) {
                column = ((SQLIdentifierExpr) betweenExpr.getTestExpr()).getName();
                tableName = getOwnerTableName(betweenExpr, column);
            }
            String table = tableName;
            if (aliasMap.containsKey(table)) {
                table = aliasMap.get(table);
            }

            if (table != null && !"".equals(table)) {
                return new Column(table, column);
            }
        }
        return null;
    }

    private Column getColumnByExpr(SQLIdentifierExpr expr) {
        String column = expr.getName();
        String table = currentTable;
        if (table != null && aliasMap.containsKey(table)) {
            table = aliasMap.get(table);
            if (table == null) {
                return null;
            }
        }

        if (table != null) {
            return new Column(table, column);
        }

        return new Column("UNKNOWN", column);
    }

    private Column getColumnByExpr(SQLPropertyExpr expr) {
        SQLExpr owner = expr.getOwner();
        String column = expr.getName();

        if (owner instanceof SQLIdentifierExpr || owner instanceof SQLPropertyExpr) {
            String tableName;
            if (owner instanceof SQLPropertyExpr) {
                tableName = ((SQLPropertyExpr) owner).getName();
                if (((SQLPropertyExpr) owner).getOwner() instanceof SQLIdentifierExpr) {
                    tableName = ((SQLIdentifierExpr) ((SQLPropertyExpr) owner).getOwner()).getName() + "." + tableName;
                }
            } else {
                tableName = ((SQLIdentifierExpr) owner).getName();
            }
            String table = tableName;
            if (aliasMap.containsKey(table)) {
                table = aliasMap.get(table);
            }
            return new Column(table, column);
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
            for (Column col : columns.values()) {
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
    private List<List<Condition>> splitConditions() {
        //pre deal with condition and whereUnits
        reBuildWhereUnits();

        this.storedWhereUnits.addAll(whereUnits);

        //split according to or expr
        for (WhereUnit whereUnit : whereUnits) {
            splitUntilNoOr(whereUnit);
        }
        loopFindSubOrCondition(storedWhereUnits);


        for (WhereUnit whereUnit : storedWhereUnits) {
            this.resetCondtionsFromWhereUnit(whereUnit);
        }

        return mergedConditions();
    }

    /**
     * Loop all the splitedExprList and try to accept them again
     * if the Expr in splitedExprList is still  a OR-Expr just deal with it
     * <p>
     * This function only recursively all child splitedExpr and make them split again
     *
     * @param whereUnitList
     */
    private void loopFindSubOrCondition(List<WhereUnit> whereUnitList) {
        List<WhereUnit> subWhereUnits = new ArrayList<>();
        for (WhereUnit whereUnit : whereUnitList) {
            if (whereUnit.getSplitedExprList().size() > 0) {
                List<SQLExpr> removeSplitedList = new ArrayList<>();
                for (SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
                    reset();
                    if (isExprHasOr(sqlExpr)) {
                        removeSplitedList.add(sqlExpr);
                        for (WhereUnit subWhereUnit : whereUnits) {
                            splitUntilNoOr(subWhereUnit);
                            whereUnit.addSubWhereUnit(subWhereUnit);
                            subWhereUnits.add(subWhereUnit);
                        }
                    } else {
                        this.relationships.clear();
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
            loopFindSubOrCondition(subWhereUnits);
        }
    }

    /**
     * reBuild WhereUnits
     */
    private void reBuildWhereUnits() {
        relationMerge(getRelationships());
        if (conditions.size() > 0) {
            for (int i = 0; i < whereUnits.size(); i++) {
                WhereUnit orgUnit = whereUnits.get(i);
                WhereUnit whereUnit = new WhereUnit();
                whereUnit.setFinishedParse(true);
                whereUnit.addOutConditions(getConditions());
                whereUnit.addOutRelationships(getRelationships());
                WhereUnit innerWhereUnit = new WhereUnit(orgUnit.getCanSplitExpr());
                whereUnit.addSubWhereUnit(innerWhereUnit);
                whereUnits.set(i, whereUnit);
            }
        }
    }

    /**
     * Equal or not equal to relationship transfer
     * E.g.  ntest.id = mtest.id and xtest.id = ntest.id
     * we try to add a derivative relationship "xtest.id = mtest.id"
     * so when there has limit in mtest.id the xtest.id can also get the limit
     * P.S.:Only order insensitive operator can be optimize,like = or <=>
     *
     * @param relationships
     */
    private void relationMerge(Set<Relationship> relationships) {
        HashSet<Relationship> loopReSet = new HashSet<>();
        loopReSet.addAll(relationships);
        for (Relationship re : loopReSet) {
            for (Relationship inv : loopReSet) {
                if (inv.getOperator().equals(re.getOperator()) && (inv.getOperator().equals("=") || inv.getOperator().equals("<=>"))) {
                    List<Column> tempSet = new ArrayList<>();
                    addAndCheckDuplicate(tempSet, re.getLeft());
                    addAndCheckDuplicate(tempSet, re.getRight());
                    addAndCheckDuplicate(tempSet, inv.getLeft());
                    addAndCheckDuplicate(tempSet, inv.getRight());
                    if (tempSet.size() == 2) {
                        Relationship rs1 = new Relationship(tempSet.get(0), tempSet.get(1), inv.getOperator());
                        Relationship rs2 = new Relationship(tempSet.get(1), tempSet.get(0), inv.getOperator());
                        if (relationships.contains(rs1) || relationships.contains(rs2)) {
                            continue;
                        } else {
                            relationships.add(rs1);
                        }
                    }
                }
            }
        }
    }

    /**
     * find how mach non-repeating column in 2 relationships
     * E.g. A = B and B = C the result size is 2
     * A = B and C = D the result size is 4
     * A = B and B = A the result size is 0
     * when the result size is 2,we can know that there is 3 columns in  2 relationships
     * and the derivative relationship may be needed
     *
     * @param tempSet
     * @param tmp
     */
    private void addAndCheckDuplicate(List<Column> tempSet, Column tmp) {
        if (tempSet.contains(tmp)) {
            tempSet.remove(tmp);
        } else {
            tempSet.add(tmp);
        }
    }

    private boolean isExprHasOr(SQLExpr expr) {
        expr.accept(this);
        reBuildWhereUnits();
        return hasOrCondition;
    }

    private List<List<Condition>> mergedConditions() {
        if (storedWhereUnits.size() == 0) {
            return new ArrayList<>();
        }

        for (WhereUnit whereUnit : storedWhereUnits) {
            mergeSubConditionWithOuterCondition(whereUnit);
        }

        return getMergedConditionList(storedWhereUnits);

    }

    /**
     * mergeSubConditionWithOuterCondition
     * Only subWhereUnit will be deal
     *
     * @param whereUnit
     */
    private void mergeSubConditionWithOuterCondition(WhereUnit whereUnit) {
        if (whereUnit.getSubWhereUnit().size() > 0) {
            for (WhereUnit sub : whereUnit.getSubWhereUnit()) {
                mergeSubConditionWithOuterCondition(sub);
            }

            if (whereUnit.getSubWhereUnit().size() > 1) {
                List<List<Condition>> mergedConditionList = getMergedConditionList(whereUnit.getSubWhereUnit());
                if (whereUnit.getOutConditions().size() > 0) {
                    for (List<Condition> aMergedConditionList : mergedConditionList) {
                        aMergedConditionList.addAll(whereUnit.getOutConditions());
                    }
                }
                if (whereUnit.getOutRelationships().size() > 0) {
                    for (List<Condition> aMergedConditionList : mergedConditionList) {
                        extendConditionsFromRelations(aMergedConditionList, whereUnit.getOutRelationships());
                    }
                }
                whereUnit.getConditionList().addAll(mergedConditionList);
            } else if (whereUnit.getSubWhereUnit().size() == 1) {
                List<List<Condition>> subConditionList = whereUnit.getSubWhereUnit().get(0).getConditionList();
                if (whereUnit.getOutConditions().size() > 0 && subConditionList.size() > 0) {
                    for (List<Condition> aSubConditionList : subConditionList) {
                        aSubConditionList.addAll(whereUnit.getOutConditions());
                    }
                }
                if (whereUnit.getOutRelationships().size() > 0 && subConditionList.size() > 0) {
                    for (List<Condition> aSubConditionList : subConditionList) {
                        extendConditionsFromRelations(aSubConditionList, whereUnit.getOutRelationships());
                    }
                }
                whereUnit.getConditionList().addAll(subConditionList);
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

    public List<String> getSelectTableList() {
        return selectTableList;
    }


    /**
     * turn all the condition in or into conditionList
     * exp (conditionA OR conditionB) into conditionList{conditionA,conditionB}
     * so the conditionA,conditionB can be group with outer conditions
     *
     * @param whereUnit
     */
    private void resetCondtionsFromWhereUnit(WhereUnit whereUnit) {
        List<List<Condition>> retList = new ArrayList<>();
        List<Condition> outSideCondition = new ArrayList<>();
        outSideCondition.addAll(conditions);
        List<Relationship> outSideRelationship = new ArrayList<>();
        outSideRelationship.addAll(relationships);
        this.conditions.clear();
        this.relationships.clear();
        for (SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
            sqlExpr.accept(this);
            List<Condition> conds = new ArrayList<>();
            conds.addAll(getConditions());
            conds.addAll(outSideCondition);
            Set<Relationship> relations = new HashSet<>();
            relations.addAll(getRelationships());
            relations.addAll(outSideRelationship);
            extendConditionsFromRelations(conds, relations);
            retList.add(conds);
            this.conditions.clear();
            this.relationships.clear();
        }
        whereUnit.setConditionList(retList);

        for (WhereUnit subWhere : whereUnit.getSubWhereUnit()) {
            resetCondtionsFromWhereUnit(subWhere);
        }
    }

    /**
     * split on conditions into whereUnit..splitedExprList
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
                addExprNotFalse(whereUnit, expr.getRight());
                if (expr.getLeft() instanceof SQLBinaryOpExpr) {
                    whereUnit.setCanSplitExpr((SQLBinaryOpExpr) expr.getLeft());
                    splitUntilNoOr(whereUnit);
                } else {
                    addExprNotFalse(whereUnit, expr.getLeft());
                }
            } else {
                addExprNotFalse(whereUnit, expr);
                whereUnit.setFinishedParse(true);
            }
        }
    }

    private void addExprNotFalse(WhereUnit whereUnit, SQLExpr expr) {
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

    private void putAliasToMap(String name, String value) {
        if (name != null) {
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                name = name.toLowerCase();
            }
            aliasMap.put(name, value);
        }
    }

    public Map<String, String> getAliasMap() {
        return aliasMap;
    }

    public String getCurrentTable() {
        return currentTable;
    }

    private void extendConditionsFromRelations(List<Condition> conds, Set<Relationship> relations) {
        List<Condition> newConds = new ArrayList<>();
        Iterator<Condition> iterator = conds.iterator();
        while (iterator.hasNext()) {
            Condition condition = iterator.next();
            if (condition.getValues().size() == 0) {
                iterator.remove();
                continue;
            }
            if (!condition.getOperator().equals("=") && !condition.getOperator().equals("<=>")) {
                continue;
            }
            Column column = condition.getColumn();
            for (Relationship relation : relations) {
                if (!condition.getOperator().equalsIgnoreCase(relation.getOperator())) {
                    continue;
                }
                if (column.equals(relation.getLeft())) {
                    Condition cond = new Condition(relation.getRight(), condition.getOperator());
                    cond.getValues().addAll(condition.getValues());
                    newConds.add(cond);
                } else if (column.equals(relation.getRight())) {
                    Condition cond = new Condition(relation.getLeft(), condition.getOperator());
                    cond.getValues().addAll(condition.getValues());
                    newConds.add(cond);
                }
            }
        }
        conds.addAll(newConds);
    }

    public List<List<Condition>> getConditionList() {
        if (this.hasOrCondition()) {
            return this.splitConditions();
        } else {
            List<Condition> conds = this.getConditions();
            Set<Relationship> relations = getRelationships();
            extendConditionsFromRelations(conds, relations);
            List<List<Condition>> result = new ArrayList<>();
            result.add(conds);
            return result;
        }
    }
}
