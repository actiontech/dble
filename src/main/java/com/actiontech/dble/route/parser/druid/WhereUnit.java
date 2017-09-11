/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.stat.TableStat.Condition;

import java.util.ArrayList;
import java.util.List;

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

    private List<WhereUnit> subWhereUnits = new ArrayList<>();

    private boolean finishedParse = false;

    public List<Condition> getOutConditions() {
        return outConditions;
    }

    public void addOutConditions(List<Condition> conditions) {
        this.outConditions.addAll(conditions);
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
