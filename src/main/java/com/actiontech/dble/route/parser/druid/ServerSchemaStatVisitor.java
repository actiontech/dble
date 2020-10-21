/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.plan.common.item.function.ItemCreate;
import com.actiontech.dble.route.util.ConditionUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.stat.TableStat;
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
    private boolean inSelect = false;
    private boolean inOuterJoin = false;
    private List<SQLSelect> subQueryList = new ArrayList<>();


    private List<SQLSelect> firstClassSubQueryList = new ArrayList();
    private Map<String, String> aliasMap = new LinkedHashMap<>();
    private Set<String> tableTables = new HashSet<>();
    private List<String> selectTableList = new ArrayList<>();
    private List<SQLExprTableSource> motifyTableSourceList = new ArrayList<>();
    private String currentTable;
    private boolean firstSelectBlock = true;
    private boolean containsInnerFunction = false;

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

    private void addSubQuery(SQLSelect subQuery) {
        if (!CollectionUtil.contaionSpecificObject(subQueryList, subQuery)) {
            subQueryList.add(subQuery);
            SQLObject ob = subQuery.getParent();
            for (; ; ob = ob.getParent()) {
                if (ob != null) {
                    if (ob instanceof SQLQueryExpr || ob instanceof SQLBinaryOpExpr || ob instanceof SQLInSubQueryExpr) {
                        continue;
                    } else if (ob instanceof MySqlUpdateStatement) {
                        firstClassSubQueryList.add(subQuery);
                    } else if (ob instanceof MySqlDeleteStatement) {
                        firstClassSubQueryList.add(subQuery);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public boolean visit(SQLInSubQueryExpr x) {
        super.visit(x);
        addSubQuery(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLQueryExpr x) {
        super.visit(x);
        addSubQuery(x.getSubQuery());
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
        addSubQuery(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLAllExpr x) {
        super.visit(x);
        addSubQuery(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLSomeExpr x) {
        super.visit(x);
        addSubQuery(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLAnyExpr x) {
        super.visit(x);
        addSubQuery(x.getSubQuery());
        return true;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        //need to protect parser SQLSelectItem, or SQLBinaryOpExpr may add to whereUnit
        // eg:id =1 will add to whereUnit
        inSelect = true;
        SQLExpr sqlExpr = x.getExpr();
        if (sqlExpr instanceof SQLMethodInvokeExpr && ItemCreate.getInstance().isInnerFunc(sqlExpr.toString().replace("(", "").replace(")", ""))) {
            containsInnerFunction = true;
        } else if (sqlExpr instanceof SQLIdentifierExpr && ItemCreate.getInstance().isInnerFunc(sqlExpr.toString())) {
            containsInnerFunction = true;
        }
        sqlExpr.accept(this);
        inSelect = false;

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
    public boolean visit(SQLJoinTableSource x) {
        switch (x.getJoinType()) {
            case LEFT_OUTER_JOIN:
            case RIGHT_OUTER_JOIN:
            case FULL_OUTER_JOIN:
                inOuterJoin = true;
                break;
            default:
                inOuterJoin = false;
                break;
        }

        SQLTableSource left = x.getLeft(), right = x.getRight();

        left.accept(this);
        right.accept(this);

        SQLExpr condition = x.getCondition();
        if (condition != null) {
            condition.accept(this);
        }

        if (x.getUsing().size() > 0 &&
                left instanceof SQLExprTableSource && right instanceof SQLExprTableSource) {
            SQLExpr leftExpr = ((SQLExprTableSource) left).getExpr();
            SQLExpr rightExpr = ((SQLExprTableSource) right).getExpr();

            for (SQLExpr expr : x.getUsing()) {
                if (expr instanceof SQLIdentifierExpr) {
                    String name = ((SQLIdentifierExpr) expr).getName();
                    /*
                    when the shard1 a join shard2 b using(id)
                    the intermediate condition should be a.id = b.id instead of shard1.id = shard2.id
                     */
                    SQLPropertyExpr leftPropExpr = new SQLPropertyExpr(leftExpr, name);
                    if (left.getAlias() != null) {
                        leftPropExpr.setOwner(left.getAlias());
                    }
                    SQLPropertyExpr rightPropExpr = new SQLPropertyExpr(rightExpr, name);
                    if (right.getAlias() != null) {
                        rightPropExpr.setOwner(right.getAlias());
                    }

                    leftPropExpr.setResolvedTableSource(left);
                    rightPropExpr.setResolvedTableSource(right);

                    SQLBinaryOpExpr usingCondition = new SQLBinaryOpExpr(leftPropExpr, SQLBinaryOperator.Equality, rightPropExpr);
                    usingCondition.accept(this);
                }
            }
        }

        inOuterJoin = false;
        return false;
    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        putAliasToMap(x.getAlias(), "subquery");
        return super.visit(x);
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

        Condition condition = new Condition(column, "between");
        this.conditions.add(condition);


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
                if (!inSelect && !inOuterJoin) {
                    handleCondition(x.getLeft(), x.getOperator().name, x.getRight());
                    handleCondition(x.getRight(), x.getOperator().name, x.getLeft());
                }
                if (!inSelect) {
                    handleRelationship(x.getLeft(), x.getOperator().name, x.getRight());
                }
                break;
            case BooleanOr:
                //remove always true
                if (!RouterUtil.isConditionAlwaysTrue(x) && !inSelect && !inOuterJoin) {
                    hasOrCondition = true;
                    WhereUnit whereUnit;
                    whereUnit = new WhereUnit(x);
                    whereUnit.addOutConditions(getConditions());
                    whereUnit.addOutRelationships(getRelationships());
                    whereUnits.add(whereUnit);
                } else if (!RouterUtil.isConditionAlwaysTrue(x) && !inSelect) {
                    WhereUnit whereUnit;
                    whereUnit = new WhereUnit(x);
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
        if (x.getTableSource() instanceof SQLJoinTableSource) {
            SQLJoinTableSource tableSource = (SQLJoinTableSource) x.getTableSource();
            if (tableSource.getRight() instanceof SQLExprTableSource) {
                String ident = tableSource.getRight().toString();
                putAliasToMap(ident, ident.replace("`", ""));
                String alias = tableSource.getRight().getAlias();
                if (alias != null) {
                    putAliasToMap(alias, ident.replace("`", ""));
                }
                motifyTableSourceList.add((SQLExprTableSource) tableSource.getRight());
            } else {
                tableSource.getRight().accept(this);
            }

            if (tableSource.getLeft() instanceof SQLExprTableSource) {
                String ident = tableSource.getLeft().toString();
                putAliasToMap(ident, ident.replace("`", ""));
                String alias = tableSource.getLeft().getAlias();
                if (alias != null) {
                    putAliasToMap(alias, ident.replace("`", ""));
                }
                motifyTableSourceList.add((SQLExprTableSource) tableSource.getLeft());
            } else {
                tableSource.getLeft().accept(this);
            }
        } else {
            SQLName identName = x.getTableName();
            if (identName != null) {
                String ident = identName.toString();
                currentTable = ident;

                putAliasToMap(ident, ident.replace("`", ""));
                String alias = x.getTableSource().getAlias();
                if (alias != null) {
                    putAliasToMap(alias, ident.replace("`", ""));
                }
            } else {
                x.getTableSource().accept(this);
            }
        }


        accept(x.getItems());
        accept(x.getWhere());

        return false;
    }

    @Override
    public boolean visit(SQLUpdateSetItem x) {
        return true;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        if (this.isSimpleExprTableSource(x)) {
            String ident = x.getExpr().toString();
            currentTable = ident;
            selectTableList.add(ident);
            motifyTableSourceList.add(x);
            String alias = x.getAlias();
            if (alias != null && !aliasMap.containsKey(alias)) {
                putAliasToMap(alias, ident.replace("`", ""));
            }

            if (!aliasMap.containsKey(ident)) {
                putAliasToMap(ident, ident.replace("`", ""));
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
        if (firstSelectBlock) {
            firstSelectBlock = false;
        } else {
            whereUnits.addAll(getAllWhereUnit());
            this.relationships.clear();
            this.conditions.clear();
            this.hasOrCondition = false;
        }
        return true;
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
            Condition condition = new Condition(column, operator);
            this.conditions.add(condition);

            SQLExpr[] var12 = valueExprs;
            int var13 = valueExprs.length;

            for (int var8 = 0; var8 < var13; ++var8) {
                SQLExpr item = var12[var8];
                Column valueColumn = this.getColumn(item);
                if (valueColumn == null) {
                    if (item instanceof SQLNullExpr) {
                        condition.getValues().add(item);
                    } else {
                        if (item instanceof SQLHexExpr) {
                            condition.getValues().add(item);
                        } else {
                            Object value = SQLEvalVisitorUtils.eval(this.getDbType(), item, this.getParameters(), false);
                            condition.getValues().add(value);
                        }
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
            } else if (betweenExpr.getTestExpr() instanceof SQLIdentifierExpr) {
                column = ((SQLIdentifierExpr) betweenExpr.getTestExpr()).getName();
                tableName = getOwnerTableName(betweenExpr, column);
            }
            if (tableName != null && !"".equals(tableName)) {
                checkAliasInColumn(tableName, false);
                return new Column(tableName, column);
            }
        }
        return null;
    }

    private Column getColumnByExpr(SQLIdentifierExpr expr) {
        String column = expr.getName();
        String table = currentTable;
        if (table != null) {
            return new Column(table, column);
        }

        return null;
    }

    private Column getColumnByExpr(SQLPropertyExpr expr) {
        SQLExpr owner = expr.getOwner();
        String column = expr.getName();
        boolean containSchema = false;
        if (owner instanceof SQLIdentifierExpr || owner instanceof SQLPropertyExpr) {
            String tableName;
            if (owner instanceof SQLPropertyExpr) {
                tableName = ((SQLPropertyExpr) owner).getName();
                if (((SQLPropertyExpr) owner).getOwner() instanceof SQLIdentifierExpr) {
                    containSchema = true;
                    tableName = ((SQLIdentifierExpr) ((SQLPropertyExpr) owner).getOwner()).getName() + "." + tableName;
                }
            } else {
                tableName = ((SQLIdentifierExpr) owner).getName();
            }
            checkAliasInColumn(tableName, containSchema);
            return new Column(tableName, column);
        }

        return null;
    }

    private void checkAliasInColumn(String tableName, boolean containSchema) {
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tableName = tableName.toLowerCase();
        }
        if (aliasMap.containsKey(tableName)) {
            return;
        }
        if (containSchema) {
            putAliasToMap(tableName, tableName.replace("`", ""));
            return;
        }
        String tempStr;
        if (StringUtil.containsApostrophe(tableName)) {
            tempStr = tableName.replace("`", "");
        } else {
            tempStr = "`" + tableName + "`";
        }
        putAliasToMap(tableName, aliasMap.getOrDefault(tempStr, tempStr));
    }

    /**
     * get table name of field in between expr
     */
    private String getOwnerTableName(SQLBetweenExpr betweenExpr, String column) {
        if (tableTables.size() == 1) { //only has 1 table
            return tableTables.iterator().next();
        } else if (tableTables.size() == 0) { //no table
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
     * Loop all the splitedExprList and try to accept them again
     * if the Expr in splitedExprList is still  a OR-Expr just deal with it
     * <p>
     * This function only recursively all child splitedExpr and make them split again
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
                        if (!relationships.contains(rs1) && !relationships.contains(rs2)) {
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

    public List<String> getSelectTableList() {
        return selectTableList;
    }


    public boolean isContainsInnerFunction() {
        return containsInnerFunction;
    }

    /**
     * turn all the condition in or into conditionList
     * exp (conditionA OR conditionB) into conditionList{conditionA,conditionB}
     * so the conditionA,conditionB can be group with outer conditions
     */
    private void resetConditionsFromWhereUnit(WhereUnit whereUnit) {
        if (!whereUnit.isFinishedExtend()) {
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
                ConditionUtil.extendConditionsFromRelations(conds, relations);
                retList.add(conds);
                this.conditions.clear();
                this.relationships.clear();
            }
            whereUnit.setOrConditionList(retList);

            for (WhereUnit subWhere : whereUnit.getSubWhereUnit()) {
                resetConditionsFromWhereUnit(subWhere);
            }
            whereUnit.setFinishedExtend(true);
        }
    }

    /**
     * split on conditions into whereUnit..splitedExprList
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
            if (expr == null) {
                whereUnit.setFinishedParse(true);
            } else if (expr.getOperator() == SQLBinaryOperator.BooleanOr) {
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
                value = value.toLowerCase();
            }
            aliasMap.put(name, value);
            tableTables.add(value);
        }
    }

    public Map<String, String> getAliasMap() {
        return aliasMap;
    }

    public String getCurrentTable() {
        return currentTable;
    }

    private static void mergeOuterRelations(WhereUnit whereUnit) {
        if (whereUnit.getSubWhereUnit().size() > 0) {
            for (WhereUnit sub : whereUnit.getSubWhereUnit()) {
                mergeOuterRelations(sub);
                if (whereUnit.getOutRelationships().size() > 0) {
                    for (List<TableStat.Condition> subConditionList : sub.getOrConditionList()) {
                        ConditionUtil.extendConditionsFromRelations(subConditionList, whereUnit.getOutRelationships());
                    }
                }
            }
        }
    }

    public List<WhereUnit> getAllWhereUnit() {
        List<WhereUnit> storedWhereUnits = new ArrayList<>();
        if (this.hasOrCondition) {
            //pre deal with condition and whereUnits
            reBuildWhereUnits();
            storedWhereUnits.addAll(whereUnits);

            //split according to or expr
            for (WhereUnit whereUnit : whereUnits) {
                splitUntilNoOr(whereUnit);
            }
            whereUnits.clear();
            loopFindSubOrCondition(storedWhereUnits);

            for (WhereUnit whereUnit : storedWhereUnits) {
                this.resetConditionsFromWhereUnit(whereUnit);
            }
        } else {
            storedWhereUnits.addAll(whereUnits);
            whereUnits.clear();
            WhereUnit whereUnit = generateWhereUnit();
            if (whereUnit != null) {
                storedWhereUnits.add(whereUnit);
            }
        }

        for (WhereUnit whereUnit : storedWhereUnits) {
            mergeOuterRelations(whereUnit);
        }

        return storedWhereUnits;
    }

    private WhereUnit generateWhereUnit() {
        List<Condition> conditionList = new ArrayList<>();
        conditionList.addAll(this.getConditions());
        ConditionUtil.extendConditionsFromRelations(conditionList, this.relationships);
        if (conditionList.size() == 0 && this.relationships.size() == 0) {
            return null;
        }
        WhereUnit whereUnit = new WhereUnit();
        whereUnit.setFinishedParse(true);
        List<List<Condition>> retList = new ArrayList<>();
        retList.add(conditionList);
        whereUnit.setOrConditionList(retList);
        whereUnit.addOutRelationships(this.relationships);
        return whereUnit;
    }

    public List<SQLSelect> getFirstClassSubQueryList() {
        return firstClassSubQueryList;
    }


    public List<WhereUnit> getWhereUnits() {
        return whereUnits;
    }


    public List<SQLExprTableSource> getMotifyTableSourceList() {
        return motifyTableSourceList;
    }
}
