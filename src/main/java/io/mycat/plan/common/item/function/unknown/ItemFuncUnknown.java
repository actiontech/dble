/**
 *
 */
package io.mycat.plan.common.item.function.unknown;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.config.ErrorCode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


/**
 * @author ActionTech
 * @CreateTime 2016/5/9
 */
public class ItemFuncUnknown extends ItemFunc {
    private final String funcName;

    /**
     * @param args
     */
    public ItemFuncUnknown(String funcName, List<Item> args) {
        super(args);
        this.funcName = funcName;
        this.withUnValAble = true;
    }

    @Override
    public void fixLengthAndDec() {
    }

    @Override
    public String funcName() {
        return this.funcName;
    }

    @Override
    public BigDecimal valReal() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", " udf func not support val()");
    }

    @Override
    public BigInteger valInt() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", " udf func not support val()");
    }

    @Override
    public String valStr() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", " udf func not support val()");
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", " udf func not support val()");
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", " udf func not support val()");
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName);
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
        return new ItemFuncUnknown(funcName, newArgs);
    }

}
