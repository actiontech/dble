/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


/**
 * args's type maybe different from each other
 */
public abstract class ItemFuncNumhybrid extends ItemFunc {
    protected ItemResult hybridType;

    public ItemFuncNumhybrid(List<Item> args) {
        super(args);
        hybridType = ItemResult.REAL_RESULT;
    }

    @Override
    public ItemResult resultType() {
        return hybridType;
    }

    @Override
    public void fixLengthAndDec() {
        fixNumLengthAndDec();
        findNumType();
    }

    public void fixNumLengthAndDec() {

    }

    /* To be called from fix_length_and_dec */
    public abstract void findNumType();

    @Override
    public BigDecimal valReal() {
        if (hybridType == ItemResult.DECIMAL_RESULT) {
            BigDecimal val = decimalOp();
            if (val == null)
                return BigDecimal.ZERO; // null is setreturn val;
        } else if (hybridType == ItemResult.INT_RESULT) {
            BigInteger result = intOp();
            return new BigDecimal(result);
        } else if (hybridType == ItemResult.REAL_RESULT) {
            return realOp();
        } else if (hybridType == ItemResult.STRING_RESULT) {
            FieldTypes i = fieldType();
            if (i == FieldTypes.MYSQL_TYPE_TIME || i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
                return valRealFromDecimal();
            }
            String res = strOp();
            if (res == null)
                return BigDecimal.ZERO;
            else {
                try {
                    return new BigDecimal(res);
                } catch (Exception e) {
                    LOGGER.error(res + " to BigDecimal error!", e);
                }
            }
        }
        return BigDecimal.ZERO;
    }

    @Override
    public BigInteger valInt() {
        if (hybridType == ItemResult.DECIMAL_RESULT) {
            BigDecimal val = decimalOp();
            if (val == null)
                return BigInteger.ZERO;
            return val.toBigInteger();
        } else if (hybridType == ItemResult.INT_RESULT) {
            return intOp();
        } else if (hybridType == ItemResult.REAL_RESULT) {
            return realOp().toBigInteger();
        } else if (hybridType == ItemResult.STRING_RESULT) {
            FieldTypes i = fieldType();
            if (i == FieldTypes.MYSQL_TYPE_DATE) {
                return new BigDecimal(valIntFromDate()).toBigInteger();
            } else if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
                return new BigDecimal(valIntFromDatetime()).toBigInteger();
            } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
                return new BigDecimal(valIntFromTime()).toBigInteger();
            }
            String res = strOp();
            if (res == null)
                return BigInteger.ZERO;
            try {
                return new BigInteger(res);
            } catch (Exception e) {
                LOGGER.error(res + " to BigInteger error!", e);
            }
        }
        return BigInteger.ZERO;
    }

    @Override
    public BigDecimal valDecimal() {
        BigDecimal val = null;
        if (hybridType == ItemResult.DECIMAL_RESULT) {
            val = decimalOp();

        } else if (hybridType == ItemResult.INT_RESULT) {
            BigInteger result = intOp();
            val = new BigDecimal(result);
        } else if (hybridType == ItemResult.REAL_RESULT) {
            BigDecimal result = realOp();
            val = result;
        } else if (hybridType == ItemResult.STRING_RESULT) {
            FieldTypes i = fieldType();
            if (i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
                return valDecimalFromDate();
            } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
                return valDecimalFromTime();
            }
            String res = strOp();
            if (res == null)
                return null;
            try {
                val = new BigDecimal(res);
            } catch (Exception e) {
                val = null;
            }
        }
        return val;
    }

    @Override
    public String valStr() {
        String str = null;
        if (hybridType == ItemResult.DECIMAL_RESULT) {
            BigDecimal val = decimalOp();
            if (val == null)
                return null; // null is setstr = val.toString();
        } else if (hybridType == ItemResult.INT_RESULT) {
            BigInteger nr = intOp();
            if (nullValue)
                return null; /* purecov: inspected */
            str = nr.toString();
        } else if (hybridType == ItemResult.REAL_RESULT) {
            BigDecimal nr = realOp();
            if (nullValue)
                return null; /* purecov: inspected */
            str = nr.toString();
        } else if (hybridType == ItemResult.STRING_RESULT) {
            FieldTypes i = fieldType();
            if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
                return valStringFromDatetime();
            } else if (i == FieldTypes.MYSQL_TYPE_DATE) {
                return valStringFromDate();
            } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
                return valStringFromTime();
            }
            return strOp();
        }
        return str;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long flags) {
        assert (fixed);
        FieldTypes i = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            return dateOp(ltime, flags);
        } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
            return getDateFromTime(ltime);
        } else {
            return getDateFromNonTemporal(ltime, flags);
        }
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        assert (fixed);
        FieldTypes i = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_TIME) {
            return timeOp(ltime);
        } else if (i == FieldTypes.MYSQL_TYPE_DATE) {
            return getTimeFromDate(ltime);
        } else if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            return getTimeFromDatetime(ltime);
        } else {
            return getTimeFromNonTemporal(ltime);
        }
    }

    /**
     * @return The result of the operation.
     * @brief Performs the operation that this functions implements when the
     * result type is INT.
     */
    public abstract BigInteger intOp();

    /**
     * @return The result of the operation.
     * @brief Performs the operation that this functions implements when the
     * result type is REAL.
     */
    public abstract BigDecimal realOp();

    /**
     * @param A pointer where the DECIMAL value will be allocated.
     * @return - 0 If the result is NULL - The same pointer it was given, with
     * the area initialized to the result of the operation.
     * @brief Performs the operation that this functions implements when the
     * result type is DECIMAL.
     */
    public abstract BigDecimal decimalOp();

    /**
     * @return The result of the operation.
     * @brief Performs the operation that this functions implements when the
     * result type is a string type.
     */
    public abstract String strOp();

    /**
     * @return The result of the operation.
     * @brief Performs the operation that this functions implements when the
     * result type is MYSQL_TYPE_DATE or MYSQL_TYPE_DATETIME.
     */
    public abstract boolean dateOp(MySQLTime ltime, long flags);

    public abstract boolean timeOp(MySQLTime ltime);

}
