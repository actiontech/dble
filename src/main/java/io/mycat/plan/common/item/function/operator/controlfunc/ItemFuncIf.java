package io.mycat.plan.common.item.function.operator.controlfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public class ItemFuncIf extends ItemFunc {
    ItemResult cachedResultType;
    FieldTypes cachedFieldType;

    public ItemFuncIf(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "if";
    }

    @Override
    public final ItemResult resultType() {
        return cachedResultType;
    }

    @Override
    public final FieldTypes fieldType() {
        return cachedFieldType;
    }

    @Override
    public int decimalPrecision() {
        int arg1Prec = args.get(1).decimalIntPart();
        int arg2Prec = args.get(2).decimalIntPart();
        int precision = Math.max(arg1Prec, arg2Prec) + decimals;
        return Math.min(precision, MySQLcom.DECIMAL_MAX_PRECISION);
    }

    @Override
    public void fixLengthAndDec() {
        // Let IF(cond, expr, NULL) and IF(cond, NULL, expr) inherit type from
        // expr.
        if (args.get(1).type() == ItemType.NULL_ITEM) {
            cache_type_info(args.get(2));
            maybeNull = true;
            // If both arguments are NULL, make resulting type BINARY(0).
            if (args.get(2).type() == ItemType.NULL_ITEM)
                cachedFieldType = FieldTypes.MYSQL_TYPE_STRING;
            return;
        }
        if (args.get(2).type() == ItemType.NULL_ITEM) {
            cache_type_info(args.get(1));
            maybeNull = true;
            return;
        }
        cachedResultType = MySQLcom.agg_result_type(args, 1, 2);
        cachedFieldType = MySQLcom.agg_field_type(args, 1, 2);
        maybeNull = args.get(1).maybeNull || args.get(2).maybeNull;
        decimals = Math.max(args.get(1).decimals, args.get(2).decimals);
    }

    @Override
    public BigDecimal valReal() {
        Item arg = args.get(0).valBool() ? args.get(1) : args.get(2);
        BigDecimal value = arg.valReal();
        nullValue = arg.nullValue;
        return value;
    }

    @Override
    public BigInteger valInt() {
        Item arg = args.get(0).valBool() ? args.get(1) : args.get(2);
        BigInteger value = arg.valInt();
        nullValue = arg.nullValue;
        return value;
    }

    @Override
    public String valStr() {
        if (fieldType() == FieldTypes.MYSQL_TYPE_DATETIME || fieldType() == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            return valStringFromDatetime();
        } else if (fieldType() == FieldTypes.MYSQL_TYPE_DATE) {
            return valStringFromDate();
        } else if (fieldType() == FieldTypes.MYSQL_TYPE_TIME) {
            return valStringFromTime();
        } else {
            Item item = args.get(0).valBool() ? args.get(1) : args.get(2);
            String res;
            if ((res = item.valStr()) != null) {
                nullValue = false;
                return res;
            }
        }
        nullValue = true;
        return null;
    }

    @Override
    public BigDecimal valDecimal() {
        Item arg = args.get(0).valBool() ? args.get(1) : args.get(2);
        BigDecimal value = arg.valDecimal();
        nullValue = arg.nullValue;
        return value;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        Item arg = args.get(0).valBool() ? args.get(1) : args.get(2);
        return (nullValue = arg.getDate(ltime, fuzzydate));
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        Item arg = args.get(0).valBool() ? args.get(1) : args.get(2);
        return (nullValue = arg.getTime(ltime));
    }

    private void cache_type_info(Item source) {
        cachedFieldType = source.fieldType();
        cachedResultType = source.resultType();
        decimals = source.decimals;
        maxLength = source.maxLength;
        maybeNull = source.maybeNull;
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        for (Item arg : args) {
            method.addParameter(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncIf(newArgs);
    }
}
