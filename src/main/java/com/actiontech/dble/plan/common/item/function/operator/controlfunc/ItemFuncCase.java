/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.controlfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public class ItemFuncCase extends ItemFunc {

    int firstExprNum, elseExprNum;
    ItemResult cachedResultType;
    int ncases;
    FieldTypes cachedFieldType;

    /**
     * @param args
     * @param firstExprNum -1 means no case exp,else means the index of args in case exp,case and else exp are at the end of args
     * @param elseExprNum  the index of else in args
     */
    public ItemFuncCase(List<Item> args, int ncases, int firstExprNum, int elseExprNum) {
        super(args);
        this.ncases = ncases;
        this.firstExprNum = firstExprNum;
        this.elseExprNum = elseExprNum;
        this.cachedResultType = ItemResult.INT_RESULT;
    }

    @Override
    public final String funcName() {
        return "case";
    }

    @Override
    public void fixLengthAndDec() {
        List<Item> agg = new ArrayList<>();
        int nagg;
        /*
         * Aggregate all THEN and ELSE expression types and collations when
         * string result
         */

        for (nagg = 0; nagg < ncases / 2; nagg++)
            agg.add(args.get(nagg * 2 + 1));
        if (elseExprNum != -1)
            agg.add(args.get(elseExprNum));
        cachedFieldType = MySQLcom.aggFieldType(agg, 0, agg.size());
        cachedResultType = MySQLcom.aggResultType(agg, 0, agg.size());
    }

    @Override
    public ItemResult resultType() {
        return cachedResultType;
    }

    @Override
    public FieldTypes fieldType() {
        return cachedFieldType;
    }

    @Override
    public BigDecimal valReal() {
        Item item = findItem();
        if (item == null) {
            nullValue = true;
            return BigDecimal.ZERO;
        }
        BigDecimal res = item.valReal();
        nullValue = item.isNullValue();
        return res;
    }

    @Override
    public BigInteger valInt() {
        Item item = findItem();
        if (item == null) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        BigInteger res = item.valInt();
        nullValue = item.isNullValue();
        return res;
    }

    @Override
    public String valStr() {
        FieldTypes i = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            return valStringFromDatetime();
        } else if (i == FieldTypes.MYSQL_TYPE_DATE) {
            return valStringFromDate();
        } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
            return valStringFromTime();
        } else {
            Item item = findItem();
            if (item != null) {
                String res;
                if ((res = item.valStr()) != null) {
                    nullValue = false;
                    return res;
                }
            }
        }
        nullValue = true;
        return null;
    }

    @Override
    public BigDecimal valDecimal() {
        Item item = findItem();
        if (item == null) {
            nullValue = true;
            return null;
        }
        BigDecimal res = item.valDecimal();
        nullValue = item.isNullValue();
        return res;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        Item item = findItem();
        if (item == null)
            return (nullValue = true);
        return (nullValue = item.getDate(ltime, fuzzydate));
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        Item item = findItem();
        if (item == null)
            return (nullValue = true);
        return (nullValue = item.getTime(ltime));
    }

    /**
     * Find and return matching items for CASE or ELSE item if all compares are
     * failed or NULL if ELSE item isn't defined.
     * <p>
     * IMPLEMENTATION In order to do correct comparisons of the CASE expression
     * (the expression between CASE and the first WHEN) with each WHEN
     * expression several comparators are used. One for each result type. CASE
     * expression can be evaluated up to # of different result types are used.
     * To check whether the CASE expression already was evaluated for a
     * particular result type a bit mapped variable value_added_map is used.
     * Result types are mapped to it according to their int values i.e.
     * STRING_RESULT is mapped to bit 0, REAL_RESULT to bit 1, so on.
     *
     * @retval NULL Nothing found and there is no ELSE expression defined
     * @retval item Found item or ELSE item if defined and all comparisons are
     * failed
     */
    private Item findItem() {
        if (firstExprNum == -1) {
            for (int i = 0; i < ncases; i += 2) {
                // No expression between CASE and the first WHEN
                if (args.get(i).valBool())
                    return args.get(i + 1);
                continue;
            }
        } else {
            /* Compare every WHEN argument with it and return the first match */
            Item leftCmpItem = args.get(firstExprNum);
            if (leftCmpItem.isNull() || leftCmpItem.type() == ItemType.NULL_ITEM) {
                return elseExprNum != -1 ? args.get(elseExprNum) : null;
            }
            for (int i = 0; i < ncases; i += 2) {
                if (args.get(i).type() == ItemType.NULL_ITEM)
                    continue;
                Item rightCmpItem = args.get(i);
                ArgComparator cmptor = new ArgComparator(leftCmpItem, rightCmpItem);
                cmptor.setCmpFunc(null, leftCmpItem, rightCmpItem, false);
                if (cmptor.compare() == 0 && !rightCmpItem.isNullValue())
                    return args.get(i + 1);
            }
        }
        // No, WHEN clauses all missed, return ELSE expression
        return elseExprNum != -1 ? args.get(elseExprNum) : null;
    }

    // @Override
    // protected Item cloneStruct() {
    // List<Item> newArgList = cloneStructList(args);
    // return new Item_func_case(newArgList, ncases, first_expr_num,
    // else_expr_num);
    // }

    @Override
    public SQLExpr toExpression() {
        SQLCaseExpr caseExpr = new SQLCaseExpr();
        List<SQLExpr> exprList = toExpressionList(args);
        for (int index = 0; index < ncases; ) {
            SQLExpr exprCond = exprList.get(index++);
            SQLExpr exprValue = exprList.get(index++);

            SQLCaseExpr.Item item = new SQLCaseExpr.Item(exprCond, exprValue);
            caseExpr.addItem(item);
        }
        if (firstExprNum > 0) {
            caseExpr.setValueExpr(exprList.get(firstExprNum));
        }
        if (elseExprNum > 0) {
            caseExpr.setElseExpr(exprList.get(elseExprNum));
        }
        return caseExpr;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncCase(newArgs, ncases, firstExprNum, elseExprNum);
    }
}
