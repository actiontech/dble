/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.stat.TableStat.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * WhereUnit
 *
 * @author wang.dw
 * @version 0.1.0
 * @date 2015-3-17 16:21:21
 * @copyright wonhigh.cn
 * <p>
 * eg:
 * SELECT id,traveldate
 * FROM   travelrecord
 * WHERE  id = 1
 * AND ( fee > 0
 * OR days > 0
 * OR ( traveldate > '2015-05-04 00:00:07.375'
 * AND ( user_id <= 2
 * OR fee = days
 * OR fee > 0 ) ) )
 * AND name = 'zhangsan'
 * ORDER  BY traveldate DESC
 * LIMIT  20
 *
 * This class represents OR expression
 * means that a whereUnit is an (conditionA OR conditionB)
 * outConditions means the real out condition like (conditionA OR conditionB) and out_condition
 * and outCondition will be split into inner conditions likes conditionA and out_condition  || conditionB and out_condition
 * stored separately into splitedExprList
 */
public class WhereUnit {

    /**
     * canSplitExpr:contains or expr
     */
    private SQLBinaryOpExpr canSplitExpr;

    private List<SQLExpr> splitedExprList = new ArrayList<>();

    private List<List<Condition>> conditionList = new ArrayList<>();
    /**
     * whereExpris not contains all where condition,consider outConditions
     */
    private List<Condition> outConditions = new ArrayList<>();

    private Set<Relationship> outRelationships = new HashSet<>();


    private List<WhereUnit> subWhereUnits = new ArrayList<>();

    private boolean finishedParse = false;

    public List<Condition> getOutConditions() {
        return outConditions;
    }

    public void addOutConditions(List<Condition> conditions) {
        this.outConditions.addAll(conditions);
    }

    public Set<Relationship> getOutRelationships() {
        return outRelationships;
    }

    public void addOutRelationships(Set<Relationship> relationships) {
        this.outRelationships.addAll(relationships);
    }


    public boolean isFinishedParse() {
        return finishedParse;
    }

    public void setFinishedParse(boolean finishedParse) {
        this.finishedParse = finishedParse;
    }

    public WhereUnit() {
    }

    public WhereUnit(SQLBinaryOpExpr whereExpr) {
        this.canSplitExpr = whereExpr;
    }

    public SQLBinaryOpExpr getCanSplitExpr() {
        return canSplitExpr;
    }

    public void setCanSplitExpr(SQLBinaryOpExpr canSplitExpr) {
        this.canSplitExpr = canSplitExpr;
    }

    public List<SQLExpr> getSplitedExprList() {
        return splitedExprList;
    }

    public void addSplitedExpr(SQLExpr splitedExpr) {
        this.splitedExprList.add(splitedExpr);
    }

    public List<List<Condition>> getConditionList() {
        return conditionList;
    }

    public void setConditionList(List<List<Condition>> conditionList) {
        this.conditionList = conditionList;
    }

    public void addSubWhereUnit(WhereUnit whereUnit) {
        this.subWhereUnits.add(whereUnit);
    }

    public List<WhereUnit> getSubWhereUnit() {
        return this.subWhereUnits;
    }
}
