/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

public final class ExpressionUtil {
    private ExpressionUtil() {
    }

    /**
     * convert Expression to DNF Expression
     *
     * @param expr
     * @return
     */
    public static SQLExpr toDNF(SQLExpr expr) {
        if (expr == null)
            return null;

        while (!isDNF(expr)) {
            SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) expr;
            if (binOpExpr.getOperator() == SQLBinaryOperator.BooleanOr) {
                expr = expandOrExpression(binOpExpr);
            } else if (binOpExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
                expr = expandAndExpression(binOpExpr);
            }
        }
        return expr;
    }


    /**
     * (A OR B) AND C OR B OR (E OR F) AND G = ((A AND C ) OR (A AND B)) OR B OR
     * ((E AND G) OR (F AND G))
     *
     * @param expr
     * @return
     */
    private static SQLBinaryOpExpr expandOrExpression(SQLBinaryOpExpr expr) {
        SQLExpr left = expr.getLeft();
        SQLExpr right = expr.getRight();
        SQLExpr leftOp = toDNF(left);
        SQLExpr rightOp = toDNF(right);
        return new SQLBinaryOpExpr(leftOp, SQLBinaryOperator.BooleanOr, rightOp);
    }

    /**
     * (A or B) and C and (D or E) = ((A and C and D) or (B and C and D)) or ((A
     * and C and E) or (B and C and E))
     *
     * @param expr
     * @return
     */
    private static SQLBinaryOpExpr expandAndExpression(SQLBinaryOpExpr expr) {
        SQLExpr left = expr.getLeft();
        SQLExpr right = expr.getRight();
        if (!isLogicalExpression(left) && !isLogicalExpression(right)) {
            return expr;
        }

        SQLBinaryOpExpr leftOp = (SQLBinaryOpExpr) toDNF(left);
        SQLBinaryOpExpr rightOp = (SQLBinaryOpExpr) toDNF(right);
        SQLBinaryOpExpr dnfExpr = null;
        if (leftOp.getOperator() == SQLBinaryOperator.BooleanOr) {
            SQLExpr subLeft = leftOp.getLeft();
            SQLBinaryOpExpr newLeft = new SQLBinaryOpExpr(subLeft, SQLBinaryOperator.BooleanAnd, rightOp);
            SQLExpr subright = leftOp.getRight();
            SQLBinaryOpExpr newRight = new SQLBinaryOpExpr(subright, SQLBinaryOperator.BooleanAnd, rightOp);
            SQLBinaryOpExpr logicOr = new SQLBinaryOpExpr(newLeft, SQLBinaryOperator.BooleanOr, newRight);
            dnfExpr = expandOrExpression(logicOr);
        } else if (rightOp.getOperator() == SQLBinaryOperator.BooleanOr) {
            SQLExpr subLeft = rightOp.getLeft();
            SQLBinaryOpExpr newLeft = new SQLBinaryOpExpr(leftOp, SQLBinaryOperator.BooleanAnd, subLeft);
            SQLExpr subright = rightOp.getRight();
            SQLBinaryOpExpr newRight = new SQLBinaryOpExpr(leftOp, SQLBinaryOperator.BooleanAnd, subright);
            SQLBinaryOpExpr logicOr = new SQLBinaryOpExpr(newLeft, SQLBinaryOperator.BooleanOr, newRight);
            dnfExpr = expandOrExpression(logicOr);
        } else {
            SQLBinaryOpExpr logicAnd = new SQLBinaryOpExpr(leftOp, SQLBinaryOperator.BooleanAnd, rightOp);
            dnfExpr = logicAnd;
        }
        return dnfExpr;
    }

    /**
     * allow (A and B) and C
     *
     * @param expr
     * @return
     */
    public static boolean isDNF(SQLExpr expr) {
        if (!isLogicalExpression(expr)) {
            return true;
        }
        SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) expr;
        SQLExpr left = binOpExpr.getLeft();
        SQLExpr right = binOpExpr.getRight();
        if (binOpExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
            boolean isAllNonLogicExpr = true;
            if (left instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr leftBinaryOp = (SQLBinaryOpExpr) left;
                if (leftBinaryOp.getOperator() == SQLBinaryOperator.BooleanOr) {
                    return false;
                }
                if (isLogicalExpression(leftBinaryOp)) {
                    isAllNonLogicExpr = false;
                }
            }
            if (right instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr rightBinaryOp = (SQLBinaryOpExpr) right;
                if (rightBinaryOp.getOperator() == SQLBinaryOperator.BooleanOr) {
                    return false;
                }
                if (isLogicalExpression(rightBinaryOp)) {
                    isAllNonLogicExpr = false;
                }
            }
            if (isAllNonLogicExpr) {
                return true;
            }
        }

        if (!isDNF(left)) {
            return false;
        }
        return isDNF(right);
    }

    private static boolean isLogicalExpression(SQLExpr expr) { //XOR?
        if (!(expr instanceof SQLBinaryOpExpr)) {
            return false;
        }
        SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) expr;
        return binOpExpr.getOperator() == SQLBinaryOperator.BooleanAnd || binOpExpr.getOperator() == SQLBinaryOperator.BooleanOr;
    }
}
