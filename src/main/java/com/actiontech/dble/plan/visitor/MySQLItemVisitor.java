/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.visitor;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.CastTarget;
import com.actiontech.dble.plan.common.CastType;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.*;
import com.actiontech.dble.plan.common.item.function.ItemCreate;
import com.actiontech.dble.plan.common.item.function.ItemFuncKeyWord;
import com.actiontech.dble.plan.common.item.function.bitfunc.*;
import com.actiontech.dble.plan.common.item.function.castfunc.ItemCharTypeCast;
import com.actiontech.dble.plan.common.item.function.castfunc.ItemFuncBinaryCast;
import com.actiontech.dble.plan.common.item.function.castfunc.ItemFuncConvCharset;
import com.actiontech.dble.plan.common.item.function.castfunc.ItemNCharTypeCast;
import com.actiontech.dble.plan.common.item.function.convertfunc.ItemCharTypeConvert;
import com.actiontech.dble.plan.common.item.function.mathsfunc.operator.*;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.controlfunc.ItemFuncCase;
import com.actiontech.dble.plan.common.item.function.operator.controlfunc.ItemFuncIf;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemFuncNot;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemFuncXor;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemFuncChar;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemFuncOrd;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemFuncTrim;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemFuncTrim.TrimTypeEnum;
import com.actiontech.dble.plan.common.item.function.sumfunc.*;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemDateAddInterval;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemExtract;
import com.actiontech.dble.plan.common.item.function.timefunc.ItemFuncTimestampDiff;
import com.actiontech.dble.plan.common.item.function.unknown.ItemFuncUnknown;
import com.actiontech.dble.plan.common.item.subquery.ItemAllAnySubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemExistsSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemInSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemScalarSubQuery;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.SQLExprParser;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLItemVisitor extends MySqlASTVisitorAdapter {
    private String currentDb;
    private final int charsetIndex;
    private final ProxyMetaManager metaManager;
    private Map<String, String> usrVariables;
    private static ThreadLocal<HashMap<SQLExprWrapper, Item>> visitCache = new InheritableThreadLocal<>();

    public MySQLItemVisitor(String currentDb, int charsetIndex, ProxyMetaManager metaManager, Map<String, String> usrVariables) {
        this.currentDb = currentDb;
        this.charsetIndex = charsetIndex;
        this.metaManager = metaManager;
        this.usrVariables = usrVariables;
    }

    protected Item item;

    public Item getItem() {
        return item;
    }

    private Item getItem(SQLExpr expr) {
        Item result = null;
        SQLExprWrapper key = new SQLExprWrapper(expr);
        if (visitCache.get() != null) {
            result = visitCache.get().get(key);
        } else {
            visitCache.set(new HashMap<>());
        }
        if (result == null) {
            MySQLItemVisitor fv = new MySQLItemVisitor(currentDb, this.charsetIndex, this.metaManager, this.usrVariables);
            expr.accept(fv);
            result = fv.getItem();
            visitCache.get().put(key, result);
        }

        return result;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public static void clearCache() {
        if (visitCache.get() != null) {
            visitCache.get().clear();
        }
    }

    @Override
    public void endVisit(MySqlCharExpr x) {
        String value = x.toString();
        item = new ItemString(value, this.charsetIndex);
        item.setItemName(value);
    }

    @Override
    public void endVisit(SQLQueryExpr x) {
        SQLSelectQuery sqlSelect = x.getSubQuery().getQuery();
        item = new ItemScalarSubQuery(currentDb, sqlSelect, metaManager, usrVariables, this.charsetIndex);
        initName(x);
        item.setItemName(item.getItemName().replaceAll("\n\\t", " "));
    }

    @Override
    public void endVisit(SQLBetweenExpr x) {
        Item itemTest = getItem(x.getTestExpr());
        Item itemBegin = getItem(x.getBeginExpr());
        Item itemEnd = getItem(x.getEndExpr());
        item = new ItemFuncBetweenAnd(itemTest, itemBegin, itemEnd, x.isNot(), this.charsetIndex);
        item.setWithSubQuery(itemTest.isWithSubQuery() || itemBegin.isWithSubQuery() || itemEnd.isWithSubQuery());
        item.setCorrelatedSubQuery(itemTest.isCorrelatedSubQuery() || itemBegin.isCorrelatedSubQuery() || itemEnd.isCorrelatedSubQuery());
        initName(x);
    }

    @Override
    public void endVisit(SQLInSubQueryExpr x) {
        boolean isNeg = x.isNot();
        Item left = getItem(x.getExpr());
        item = new ItemInSubQuery(currentDb, x.getSubQuery().getQuery(), left, isNeg, metaManager, usrVariables, this.charsetIndex);
        initName(x);
        item.setItemName(item.getItemName().replaceAll("\n\\t", " "));
    }

    @Override
    public void endVisit(SQLBooleanExpr x) {
        if (x.getValue()) {
            item = new ItemInt(1);
        } else {
            item = new ItemInt(0);
        }
        initName(x);
    }

    @Override
    public void endVisit(SQLBinaryOpExpr x) {
        Item itemLeft = getItem(x.getLeft());
        SQLExpr rightExpr = x.getRight();
        Item itemRight = getItem();
        if (itemRight instanceof ItemInSubQuery && (rightExpr instanceof SQLSomeExpr || rightExpr instanceof SQLAllExpr || rightExpr instanceof SQLAnyExpr)) {
            item = itemRight;
            initName(x);
            item.setItemName(item.getItemName().replaceAll("\n\\t", " "));
            return;
        }
        switch (x.getOperator()) {
            case Is:
                // is null, or is unknown
                if (itemRight instanceof ItemNull || itemRight instanceof ItemString) {
                    item = new ItemFuncIsnull(itemLeft, this.charsetIndex);
                } else if (itemRight instanceof ItemInt) {
                    ItemInt itemBool = (ItemInt) itemRight;
                    if (itemBool.valInt().longValue() == 1) { // is true
                        item = new ItemFuncIstrue(itemLeft, this.charsetIndex);
                    } else {
                        item = new ItemFuncIsfalse(itemLeft, this.charsetIndex);
                    }
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support type:" + x.getRight());
                }
                break;
            case IsNot:
                // is not null, or is not unknown
                if (itemRight instanceof ItemNull || itemRight instanceof ItemString) {
                    item = new ItemFuncIsnotnull(itemLeft, this.charsetIndex);
                } else if (itemRight instanceof ItemInt) {
                    ItemInt itemBool = (ItemInt) itemRight;
                    if (itemBool.valInt().longValue() == 1) { // is true
                        item = new ItemFuncIsnottrue(itemLeft, this.charsetIndex);
                    } else {
                        item = new ItemFuncIsnotfalse(itemLeft, this.charsetIndex);
                    }
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support type:" + x.getRight());
                }
                break;
            case Escape:
                if (itemLeft instanceof ItemFuncLike) {
                    // A LIKE B ESCAPE C ,A is "itemLeft"
                    SQLBinaryOpExpr like = (SQLBinaryOpExpr) (x.getLeft());
                    Item itemLikeLeft = getItem(like.getLeft());
                    Item itemLikeRight = getItem(like.getRight());
                    boolean isNot = (like.getOperator() == SQLBinaryOperator.NotLike);
                    item = new ItemFuncLike(itemLikeLeft, itemLikeRight, itemRight, isNot, this.charsetIndex);
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                            "not supported kind expression:" + x.getOperator());
                }
                break;
            case NotLike:
                item = new ItemFuncLike(itemLeft, itemRight, null, true, this.charsetIndex);
                break;
            case Like:
                item = new ItemFuncLike(itemLeft, itemRight, null, false, this.charsetIndex);
                break;
            case Equality:
                item = new ItemFuncEqual(itemLeft, itemRight, this.charsetIndex);
                break;
            case Add:
                item = new ItemFuncPlus(itemLeft, itemRight, this.charsetIndex);
                break;
            case Divide:
                item = new ItemFuncDiv(itemLeft, itemRight, this.charsetIndex);
                break;
            case DIV:
                item = new ItemFuncIntDiv(itemLeft, itemRight, this.charsetIndex);
                break;
            case Mod:
            case Modulus:
                item = new ItemFuncMod(itemLeft, itemRight, this.charsetIndex);
                break;
            case Multiply:
                item = new ItemFuncMul(itemLeft, itemRight, this.charsetIndex);
                break;
            case Subtract:
                item = new ItemFuncMinus(itemLeft, itemRight, this.charsetIndex);
                break;
            case PG_And:
            case BooleanAnd:
                List<Item> argsAnd = new ArrayList<>();
                argsAnd.add(itemLeft);
                argsAnd.add(itemRight);
                item = new ItemCondAnd(argsAnd);
                break;
            case Concat:
            case BooleanOr:
                List<Item> argsOr = new ArrayList<>();
                argsOr.add(itemLeft);
                argsOr.add(itemRight);
                item = new ItemCondOr(argsOr);
                break;
            case BooleanXor:
                item = new ItemFuncXor(itemLeft, itemRight, this.charsetIndex);
                break;
            case BitwiseAnd:
                item = new ItemFuncBitAnd(itemLeft, itemRight, this.charsetIndex);
                break;
            case BitwiseOr:
                item = new ItemFuncBitOr(itemLeft, itemRight, this.charsetIndex);
                break;
            case BitwiseXor:
                item = new ItemFuncBitXor(itemLeft, itemRight, this.charsetIndex);
                break;
            case LeftShift:
                item = new ItemFuncLeftShift(itemLeft, itemRight, this.charsetIndex);
                break;
            case RightShift:
                item = new ItemFuncRightShift(itemLeft, itemRight, this.charsetIndex);
                break;
            case GreaterThan:
                item = new ItemFuncGt(itemLeft, itemRight, this.charsetIndex);
                break;
            case GreaterThanOrEqual:
                item = new ItemFuncGe(itemLeft, itemRight, this.charsetIndex);
                break;
            case NotEqual:
            case LessThanOrGreater:
                item = new ItemFuncNe(itemLeft, itemRight, this.charsetIndex);
                break;
            case LessThan:
                item = new ItemFuncLt(itemLeft, itemRight, this.charsetIndex);
                break;
            case LessThanOrEqual:
                item = new ItemFuncLe(itemLeft, itemRight, this.charsetIndex);
                break;
            case LessThanOrEqualOrGreaterThan:
                item = new ItemFuncStrictEqual(itemLeft, itemRight, this.charsetIndex);
                break;
            case RegExp:
                item = new ItemFuncRegex(itemLeft, itemRight, this.charsetIndex);
                break;
            case NotRegExp:
                item = new ItemFuncNot(new ItemFuncRegex(itemLeft, itemRight, this.charsetIndex), this.charsetIndex);
                break;
            case Assignment:
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support assignment");
            default:
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported kind expression:" + x.getOperator());
        }
        item.setWithSubQuery(itemLeft.isWithSubQuery() || itemRight.isWithSubQuery());
        item.setCorrelatedSubQuery(itemLeft.isCorrelatedSubQuery() || itemRight.isCorrelatedSubQuery());
        initName(x);
        item.setItemName(item.getItemName().replaceAll("\n\\t", " "));
    }

    @Override
    public void endVisit(SQLUnaryExpr x) {
        Item a = getItem(x.getExpr());
        switch (x.getOperator()) {
            case Negative:
                item = new ItemFuncNeg(a, this.charsetIndex);
                break;
            case Not:
            case NOT:
                item = new ItemFuncNot(a, this.charsetIndex);
                break;
            case Compl:
                item = new ItemFuncBitInversion(a, this.charsetIndex);
                break;
            case Plus:
                item = a;
                break;
            case BINARY:
                item = new ItemFuncBinaryCast(a, -1, this.charsetIndex);
                break;
            default:
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "not supported kind expression:" + x.getOperator());
        }
        item.setWithSubQuery(a.isWithSubQuery());
        item.setCorrelatedSubQuery(a.isCorrelatedSubQuery());
        initName(x);
    }

    @Override
    public void endVisit(SQLInListExpr x) {
        boolean isNeg = x.isNot();
        Item left = getItem(x.getExpr());
        List<Item> args = new ArrayList<>();
        args.add(left);
        args.addAll(visitExprList(x.getTargetList()));
        item = new ItemFuncIn(args, isNeg, this.charsetIndex);
        initName(x);
    }

    @Override
    public void endVisit(MySqlExtractExpr x) {
        item = new ItemExtract(getItem(x.getValue()), x.getUnit(), this.charsetIndex);
        initName(x);
    }

    @Override
    public void endVisit(SQLIntervalExpr x) {
        //Just as  placeholder
        item = new ItemString(x.toString(), this.charsetIndex);
        item.setItemName(x.toString());
    }

    @Override
    public void endVisit(SQLNotExpr x) {
        Item arg = getItem(x.getExpr());
        item = new ItemFuncNot(arg, this.charsetIndex);
        item.setWithSubQuery(arg.isWithSubQuery());
        item.setCorrelatedSubQuery(arg.isCorrelatedSubQuery());
        initName(x);
    }

    @Override
    public void endVisit(SQLAllColumnExpr x) {
        item = new ItemField(null, null, "*", charsetIndex);
        initName(x);
    }

    @Override
    public void endVisit(SQLCaseExpr x) {
        List<SQLCaseExpr.Item> whenList = x.getItems();
        ArrayList<Item> args = new ArrayList<>();
        int nCases, firstExprNum = -1, elseExprNum = -1;
        for (SQLCaseExpr.Item when : whenList) {
            args.add(getItem(when.getConditionExpr()));
            args.add(getItem(when.getValueExpr()));
        }
        nCases = args.size();
        // add compared
        SQLExpr compared = x.getValueExpr();
        if (compared != null) {
            firstExprNum = args.size();
            args.add(getItem(compared));
        }

        // add else exp
        SQLExpr elseExpr = x.getElseExpr();
        if (elseExpr != null) {
            elseExprNum = args.size();
            args.add(getItem(elseExpr));
        }
        item = new ItemFuncCase(args, nCases, firstExprNum, elseExprNum, this.charsetIndex);
    }

    @Override
    public void endVisit(SQLCastExpr x) {
        Item a = getItem(x.getExpr());
        SQLDataType dataType = x.getDataType();

        if (dataType instanceof SQLCharacterDataType) {
            SQLCharacterDataType charType = (SQLCharacterDataType) dataType;
            String upType = charType.getName().toUpperCase();
            List<Integer> args = changeExprListToInt(charType.getArguments());
            String charSetName = charType.getCharSetName();
            if (upType.equals("CHAR")) {
                int len = -1;
                if (args.size() > 0) {
                    len = args.get(0);
                }
                item = new ItemCharTypeCast(a, len, charSetName, this.charsetIndex);
            } else if (charSetName == null) {
                int len = -1;
                if (args.size() > 0) {
                    len = args.get(0);
                }
                item = new ItemNCharTypeCast(a, len, this.charsetIndex);
            } else {
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "",
                        "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'character set " + charSetName + ")'");
            }
        } else {
            CastType castType = getCastType((SQLDataTypeImpl) dataType);
            item = ItemCreate.getInstance().createFuncCast(a, castType);
        }
        initName(x);
    }

    @Override
    public void endVisit(SQLCharExpr x) {
        item = new ItemString(x.getText(), this.charsetIndex);
        initName(x);
    }

    @Override
    public void endVisit(SQLIdentifierExpr x) {
        item = new ItemField(null, null, StringUtil.removeBackQuote(x.getSimpleName()), charsetIndex);
    }

    @Override
    public void endVisit(SQLNullExpr x) {
        item = new ItemNull();
        initName(x);
    }

    @Override
    public void endVisit(SQLIntegerExpr x) {
        Number number = x.getNumber();
        item = new ItemInt(number.longValue());
        initName(x);
    }

    @Override
    public void endVisit(SQLNCharExpr x) {
        item = new ItemString(x.getText(), this.charsetIndex);
        initName(x);
    }

    @Override
    public void endVisit(SQLNumberExpr x) {
        Number number = x.getNumber();
        if (number instanceof BigDecimal) {
            item = new ItemDecimal((BigDecimal) number);
        } else if (number instanceof Float) {
            item = new ItemFloat(new BigDecimal(Float.toString((Float) number)));
        } else {
            item = new ItemInt(number.longValue());
        }
        initName(x);
    }

    @Override
    public void endVisit(SQLVariantRefExpr x) {
        String variable = x.getName();
        if (this.usrVariables != null) {
            String realValue = this.usrVariables.get(variable.toUpperCase());
            if (realValue != null) {
                try {
                    long value = Long.parseLong(realValue);
                    item = new ItemInt(value);
                    item.setItemName(realValue);
                    return;
                } catch (NumberFormatException e) {
                    //ignore error
                }
                try {
                    Float.parseFloat(realValue);
                    item = new ItemFloat(new BigDecimal(realValue));
                    item.setItemName(realValue);
                    return;
                } catch (NumberFormatException e) {
                    //ignore error
                }
                item = new ItemString(realValue, this.charsetIndex);
                item.setItemName(realValue);
            } else {
                item = new ItemNull();
                initName(x);
            }
        } else {
            item = new ItemVariables(x.getName(), new ItemField(null, null, variable, charsetIndex));
            initName(x);
        }
    }

    @Override
    public void endVisit(SQLPropertyExpr x) {
        String dbName = null;
        String tableName;
        if (x.getOwner() instanceof SQLPropertyExpr) {
            SQLPropertyExpr tableInfo = (SQLPropertyExpr) x.getOwner();
            dbName = ((SQLIdentifierExpr) tableInfo.getOwner()).getSimpleName();
            tableName = tableInfo.getSimpleName();
        } else {
            tableName = ((SQLIdentifierExpr) x.getOwner()).getSimpleName();
        }
        item = new ItemField(dbName, StringUtil.removeBackQuote(tableName), StringUtil.removeBackQuote(x.getSimpleName()), charsetIndex);
    }

    @Override
    public void endVisit(SQLAggregateExpr x) {
        List<Item> args = visitExprList(x.getArguments());
        String funcName = x.getMethodName().toUpperCase();
        SQLAggregateOption option = x.getOption();
        boolean isDistinct = option != null;
        switch (funcName) {
            case "MAX":
                item = new ItemSumMax(args, false, null, this.charsetIndex);
                break;
            case "MIN":
                item = new ItemSumMin(args, false, null, this.charsetIndex);
                break;
            case "SUM":
                item = new ItemSumSum(args, isDistinct, false, null, this.charsetIndex);
                break;
            case "AVG":
                item = new ItemSumAvg(args, isDistinct, false, null, this.charsetIndex);
                break;
            case "GROUP_CONCAT":
                SQLOrderBy orderExpr = (SQLOrderBy) x.getAttribute(ItemFuncKeyWord.ORDER_BY);
                List<Order> orderList = null;
                if (orderExpr != null) {
                    orderList = new ArrayList<>();
                    for (SQLSelectOrderByItem orderItem : orderExpr.getItems()) {
                        Order order;
                        if (orderItem.getType() == null) {
                            order = new Order(getItem(orderItem.getExpr()));
                        } else {
                            order = new Order(getItem(orderItem.getExpr()), orderItem.getType());
                        }
                        orderList.add(order);
                    }
                }
                SQLCharExpr charExpr = (SQLCharExpr) x.getAttribute(ItemFuncKeyWord.SEPARATOR);
                String separator = ",";
                if (charExpr != null) {
                    separator = charExpr.getText();
                }
                item = new ItemFuncGroupConcat(args, isDistinct, orderList, separator, false, null, this.charsetIndex);
                break;
            case "COUNT":
                item = new ItemSumCount(args, isDistinct, false, null, this.charsetIndex);
                break;
            case "STDDEV":
                item = new ItemSumStd(args, 0, false, null, this.charsetIndex);
                break;
            default:
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported " + funcName);
        }
        initName(x);
    }

    @Override
    public void endVisit(SQLMethodInvokeExpr x) {
        List<Item> args = visitExprList(x.getParameters());
        String funcName = x.getMethodName().toUpperCase();
        Map<String, Object> attributes = x.getAttributes();
        switch (funcName) {
            case "TRIM":
                SQLExpr from = x.getFrom();
                if (from == null) {
                    item = new ItemFuncTrim(args.get(0), TrimTypeEnum.DEFAULT, this.charsetIndex);
                } else {
                    TrimTypeEnum trimType = TrimTypeEnum.DEFAULT;
                    String trimOption = x.getTrimOption();
                    if (trimOption != null) {
                        trimType = TrimTypeEnum.valueOf(trimOption);
                    }
                    item = new ItemFuncTrim(args.get(0), getItem(from), trimType, this.charsetIndex);
                }
                break;
            case "CONVERT":
                if (args.size() >= 2) {
                    if ((args.get(1) instanceof ItemFuncChar)) {
                        ItemFuncChar charfunc = (ItemFuncChar) (args.get(1));
                        int castLength = -1;
                        try {
                            castLength = charfunc.arguments().get(0).valInt().intValue();
                        } catch (Exception e) {
                            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported  CONVERT(expr, " + args.get(1).getItemName() + ") ,please use CAST(expr AS type)");
                        }
                        item = new ItemCharTypeConvert(args.get(0), castLength, null, this.charsetIndex);
                        break;
                    }
                    CastType castType = getCastType(args.get(1).getItemName());
                    if (castType == null) {
                        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported  CONVERT(expr, " + args.get(1).getItemName() + ") ,please use CAST(expr AS type)");
                    } else {
                        item = ItemCreate.getInstance().createFuncConvert(args.get(0), castType);
                    }
                } else {
                    SQLExpr using = x.getUsing();
                    if (using == null || !(using instanceof SQLIdentifierExpr)) {
                        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "CONVERT(... USING ...) is standard SQL syntax,You should set correct charset");
                    }
                    item = new ItemFuncConvCharset(args.get(0), ((SQLIdentifierExpr) using).getSimpleName(), this.charsetIndex);
                }
                break;
            case "CHAR":
                if (attributes == null || attributes.get(ItemFuncKeyWord.USING) == null) {
                    attributes = x.getParameters().get(0).getAttributes();
                }
                if (attributes == null || attributes.get(ItemFuncKeyWord.USING) == null) {
                    item = new ItemFuncChar(args, this.charsetIndex);
                } else {
                    item = new ItemFuncChar(args, (String) attributes.get(ItemFuncKeyWord.USING), this.charsetIndex);
                }
                break;
            case "ORD":
                item = new ItemFuncOrd(args, this.charsetIndex);
                break;
            case "ADDDATE":
                if (x.getParameters().get(1) instanceof SQLIntegerExpr) {
                    item = new ItemDateAddInterval(args.get(0), args.get(1), SQLIntervalUnit.DAY, false, this.charsetIndex);
                    break;
                }
                // fallthrough
            case "DATE_ADD":
                SQLIntervalExpr intervalExpr = (SQLIntervalExpr) (x.getParameters().get(1));
                item = new ItemDateAddInterval(args.get(0), getItem(intervalExpr.getValue()), getIntervalUnit(x.getParameters().get(1)), false, this.charsetIndex);
                break;
            case "SUBDATE":
                if (x.getParameters().get(1) instanceof SQLIntegerExpr) {
                    item = new ItemDateAddInterval(args.get(0), args.get(1), SQLIntervalUnit.DAY, true, this.charsetIndex);
                    break;
                }
                // fallthrough
            case "DATE_SUB":
                SQLIntervalExpr valueExpr = (SQLIntervalExpr) (x.getParameters().get(1));
                item = new ItemDateAddInterval(args.get(0), getItem(valueExpr.getValue()), getIntervalUnit(x.getParameters().get(1)), true, this.charsetIndex);
                break;
            case "TIMESTAMPADD":
                SQLIdentifierExpr addUnit = (SQLIdentifierExpr) x.getParameters().get(0);
                item = new ItemDateAddInterval(args.get(2), args.get(1), SQLIntervalUnit.valueOf(addUnit.getSimpleName().toUpperCase()), false, this.charsetIndex);
                break;
            case "TIMESTAMPDIFF":
                SQLIdentifierExpr diffUnit = (SQLIdentifierExpr) x.getParameters().get(0);
                item = new ItemFuncTimestampDiff(args.get(1), args.get(2), SQLIntervalUnit.valueOf(diffUnit.getSimpleName().toUpperCase()), this.charsetIndex);
                break;
            case "VAR_SAMP":
                item = new ItemSumVariance(args, 1, false, null, this.charsetIndex);
                break;
            case "VAR_POP":
            case "VARIANCE":
                item = new ItemSumVariance(args, 0, false, null, this.charsetIndex);
                break;
            case "STD":
            case "STDDEV":
            case "STDDEV_POP":
                item = new ItemSumStd(args, 0, false, null, this.charsetIndex);
                break;
            case "STDDEV_SAMP":
                item = new ItemSumStd(args, 1, false, null, this.charsetIndex);
                break;
            case "BIT_AND":
                item = new ItemSumAnd(args, false, null, this.charsetIndex);
                break;
            case "BIT_OR":
                item = new ItemSumOr(args, false, null, this.charsetIndex);
                break;
            case "BIT_XOR":
                item = new ItemSumXor(args, false, null, this.charsetIndex);
                break;
            case "IF":
                item = new ItemFuncIf(args, this.charsetIndex);
                break;
            case "GET_FORMAT":
                SQLExpr expr = x.getParameters().get(0);
                if (expr instanceof SQLIdentifierExpr) {
                    Item arg0 = new ItemString(((SQLIdentifierExpr) expr).getName(), this.charsetIndex);
                    args.set(0, arg0);
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '" + expr.toString() + "'");
                }
                item = ItemCreate.getInstance().createNativeFunc(funcName, args, charsetIndex);
                break;
            default:
                if (ItemCreate.getInstance().isNativeFunc(funcName)) {
                    item = ItemCreate.getInstance().createNativeFunc(funcName, args, charsetIndex);
                } else if (ItemCreate.getInstance().isInnerFunc(funcName)) {
                    item = ItemCreate.getInstance().createInnerFunc(funcName, args, charsetIndex);
                } else {
                    // unKnownFunction
                    item = new ItemFuncUnknown(funcName, args, this.charsetIndex);
                    if (x.getParent() instanceof SQLSelectItem) {
                        if (x.getParent().getParent() instanceof MySqlSelectQueryBlock) {
                            if (((MySqlSelectQueryBlock) x.getParent().getParent()).getFrom() != null) {
                                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Unknown function " + funcName);
                            }
                        }
                    }
                }
                initName(x);
        }
        item.setWithSubQuery(getArgsSubQueryStatus(args));
        item.setCorrelatedSubQuery(getArgsCorrelatedSubQueryStatus(args));
    }

    @Override
    public void endVisit(SQLListExpr x) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Row Sub Queries is not supported");
    }

    @Override
    public void endVisit(SQLAllExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLBinaryOpExpr) {
            handleAnySubQuery((SQLBinaryOpExpr) parent, x.getSubQuery().getQuery(), true);
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                    "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near all");
        }
    }

    @Override
    public void endVisit(SQLSomeExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLBinaryOpExpr) {
            handleAnySubQuery((SQLBinaryOpExpr) parent, x.getSubQuery().getQuery(), false);
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                    "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near all");
        }
    }


    @Override
    public void endVisit(SQLAnyExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLBinaryOpExpr) {
            handleAnySubQuery((SQLBinaryOpExpr) parent, x.getSubQuery().getQuery(), false);
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                    "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near all");
        }
    }

    @Override
    public void endVisit(SQLExistsExpr x) {
        SQLSelectQuery sqlSelect = x.getSubQuery().getQuery();
        item = new ItemExistsSubQuery(currentDb, sqlSelect, x.isNot(), metaManager, usrVariables, this.charsetIndex);
        initName(x);
        item.setItemName(item.getItemName().replaceAll("\n\\t", " "));
    }

    @Override
    public void endVisit(SQLBinaryExpr x) {
        String binary = x.getText();
        if (StringUtil.equals(binary, "")) {
            item = new ItemString(binary, this.charsetIndex);
        } else {
            try {
                item = new ItemInt(Long.parseLong(binary, 2));
            } catch (NumberFormatException e) {
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near " + x.toString());
            }
        }
        initName(x);
    }

    @Override
    public void endVisit(SQLHexExpr x) {
        byte[] bytes = x.toBytes();
        if (bytes == null) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'X'" + x.getHex() + "''");
        }
        try {
            item = new ItemString(new String(bytes, CharsetUtil.getJavaCharset(this.charsetIndex)), this.charsetIndex);
        } catch (UnsupportedEncodingException e) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "Not Support charset index =" + this.charsetIndex);
        }

    }

    @Override
    public void endVisit(SQLSelectStatement node) {
        SQLSelectQuery sqlSelect = node.getSelect().getQuery();
        item = new ItemScalarSubQuery(currentDb, sqlSelect, metaManager, usrVariables, this.charsetIndex);
    }


    private void handleAnySubQuery(SQLBinaryOpExpr parent, SQLSelectQuery sqlSelect, boolean isAll) {
        SQLBinaryOperator operator = parent.getOperator();
        switch (operator) {
            case Equality:
                if (isAll) {
                    item = new ItemAllAnySubQuery(currentDb, sqlSelect, operator, true, metaManager, usrVariables, this.charsetIndex);
                } else {
                    Item left = getItem(parent.getLeft());
                    item = new ItemInSubQuery(currentDb, sqlSelect, left, false, metaManager, usrVariables, this.charsetIndex);
                }
                break;
            case NotEqual:
            case LessThanOrGreater:
                if (isAll) {
                    Item left = getItem(parent.getLeft());
                    item = new ItemInSubQuery(currentDb, sqlSelect, left, true, metaManager, usrVariables, this.charsetIndex);
                } else {
                    item = new ItemAllAnySubQuery(currentDb, sqlSelect, operator, false, metaManager, usrVariables, this.charsetIndex);
                }
                break;
            case LessThan:
            case LessThanOrEqual:
            case GreaterThan:
            case GreaterThanOrEqual:
                item = new ItemAllAnySubQuery(currentDb, sqlSelect, operator, isAll, metaManager, usrVariables, this.charsetIndex);
                break;
            default:
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near all");

        }

    }

    private CastType getCastType(String dataType) {
        SQLExprParser expr = new SQLExprParser(dataType);
        SQLDataType dataTypeImpl = expr.parseDataType();
        if (dataTypeImpl == null) {
            return null;
        }
        return getCastType((SQLDataTypeImpl) dataTypeImpl);
    }

    private CastType getCastType(SQLDataTypeImpl dataTypeImpl) {
        CastType castType = new CastType();
        String upType = dataTypeImpl.getName().toUpperCase();
        List<Integer> args = changeExprListToInt(dataTypeImpl.getArguments());
        switch (upType) {
            case "BINARY":
                castType.setTarget(CastTarget.ITEM_CAST_BINARY);
                if (args.size() > 0) {
                    castType.setLength(args.get(0));
                }
                break;
            case "DATE":
                castType.setTarget(CastTarget.ITEM_CAST_DATE);
                break;
            case "DATETIME":
                castType.setTarget(CastTarget.ITEM_CAST_DATETIME);
                if (args.size() > 0) {
                    castType.setLength(args.get(0));
                }
                break;
            case "DECIMAL":
                castType.setTarget(CastTarget.ITEM_CAST_DECIMAL);
                if (args.size() > 0) {
                    castType.setLength(args.get(0));
                }
                if (args.size() > 1) {
                    castType.setDec(args.get(1));
                }
                break;
            case "NCHAR":
                castType.setTarget(CastTarget.ITEM_CAST_NCHAR);
                if (args.size() > 0) {
                    castType.setLength(args.get(0));
                }
                break;
            case "CHAR":
                castType.setTarget(CastTarget.ITEM_CAST_CHAR);
                if (args.size() > 0) {
                    castType.setLength(args.get(0));
                }
                break;
            case "SIGNED":
                castType.setTarget(CastTarget.ITEM_CAST_SIGNED_INT);
                break;
            case "UNSIGNED":
                castType.setTarget(CastTarget.ITEM_CAST_UNSIGNED_INT);
                break;
            case "TIME":
                castType.setTarget(CastTarget.ITEM_CAST_TIME);
                if (args.size() > 0) {
                    castType.setLength(args.get(0));
                }
                break;
            default:
                // not support SIGNED INT /UNSIGNED INT/JSON
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported cast as:" + upType);
        }
        return castType;
    }

    private boolean getArgsSubQueryStatus(List<Item> args) {
        for (Item arg : args) {
            if (arg.isWithSubQuery()) {
                return true;
            }
        }
        return false;
    }

    private boolean getArgsCorrelatedSubQueryStatus(List<Item> args) {
        for (Item arg : args) {
            if (arg.isCorrelatedSubQuery()) {
                return true;
            }
        }
        return false;
    }

    private List<Integer> changeExprListToInt(List<SQLExpr> exprList) {
        List<Integer> args = new ArrayList<>();
        for (SQLExpr expr : exprList) {
            Number num = ((SQLNumericLiteralExpr) expr).getNumber();
            args.add(num.intValue());
        }
        return args;
    }

    private List<Item> visitExprList(List<SQLExpr> exprList) {
        List<Item> args = new ArrayList<>();
        for (SQLExpr expr : exprList) {
            args.add(getItem(expr));
        }
        return args;
    }

    private SQLIntervalUnit getIntervalUnit(SQLExpr expr) {
        return ((SQLIntervalExpr) expr).getUnit();
    }

    private void initName(SQLExpr expr) {
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor ov = new MySqlOutputVisitor(sb);
        ov.setShardingSupport(false);
        expr.accept(ov);
        item.setItemName(sb.toString());
    }

    private static class SQLExprWrapper {

        private final SQLExpr expr;

        SQLExprWrapper(SQLExpr expr) {
            this.expr = expr;
        }

        public SQLExpr getExpr() {
            return expr;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SQLExprWrapper) {
                return expr == ((SQLExprWrapper) obj).getExpr();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return expr.hashCode();
        }
    }
}
