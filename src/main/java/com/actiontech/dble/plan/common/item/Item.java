/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.plan.common.field.TypeConversionStatus;
import com.actiontech.dble.plan.common.time.*;
import com.actiontech.dble.plan.node.PlanNode;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public abstract class Item {

    protected static final Logger LOGGER = LoggerFactory.getLogger(Item.class);

    public static final int NOT_FIXED_DEC = 31;
    public static final int DECIMAL_MAX_SCALE = 30;
    public static final String FNAF = "$$_";

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }

    public boolean isMaybeNull() {
        return maybeNull;
    }

    public void setMaybeNull(boolean maybeNull) {
        this.maybeNull = maybeNull;
    }

    public boolean isNullValue() {
        return nullValue;
    }

    public void setNullValue(boolean nullValue) {
        this.nullValue = nullValue;
    }

    public boolean isWithSumFunc() {
        return withSumFunc;
    }

    public void setWithSumFunc(boolean withSumFunc) {
        this.withSumFunc = withSumFunc;
    }

    public boolean isWithIsNull() {
        return withIsNull;
    }

    public void setWithIsNull(boolean withIsNull) {
        this.withIsNull = withIsNull;
    }

    public boolean isWithSubQuery() {
        return withSubQuery;
    }

    public void setWithSubQuery(boolean withSubQuery) {
        this.withSubQuery = withSubQuery;
    }

    public boolean isCorrelatedSubQuery() {
        return correlatedSubQuery;
    }

    public void setCorrelatedSubQuery(boolean correlatedSubQuery) {
        this.correlatedSubQuery = correlatedSubQuery;
    }

    public boolean isWithUnValAble() {
        return withUnValAble;
    }

    public void setWithUnValAble(boolean withUnValAble) {
        this.withUnValAble = withUnValAble;
    }

    public boolean isFixed() {
        return fixed;
    }

    public void setFixed(boolean fixed) {
        this.fixed = fixed;
    }

    public ItemResult getCmpContext() {
        return cmpContext;
    }

    //TODO DELETE
    public void setCmpContext(ItemResult cmpContext) {
        this.cmpContext = cmpContext;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }


    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }


    public enum ItemResult {
        STRING_RESULT, REAL_RESULT, INT_RESULT, ROW_RESULT, DECIMAL_RESULT
    }

    public enum ItemType {
        FIELD_ITEM, FUNC_ITEM, SUM_FUNC_ITEM, STRING_ITEM, INT_ITEM, REAL_ITEM, NULL_ITEM, VARBIN_ITEM, COPY_STR_ITEM, FIELD_AVG_ITEM, DEFAULT_VALUE_ITEM, PROC_ITEM, COND_ITEM, REF_ITEM, FIELD_STD_ITEM, FIELD_VARIANCE_ITEM, INSERT_VALUE_ITEM, SUBSELECT_ITEM, ROW_ITEM, CACHE_ITEM, TYPE_HOLDER, PARAM_ITEM, TRIGGER_FIELD_ITEM, DECIMAL_ITEM, XPATH_NODESET, XPATH_NODESET_CMP, VIEW_FIXER_ITEM, VARIABLE_ITEM
    }

    protected String itemName; /* Name from visit */
    protected String pushDownName; /* name in child or db */
    protected String aliasName; /* name for alias */
    protected int maxLength = 0;
    protected int decimals = NOT_FIXED_DEC;
    protected boolean maybeNull; /* If item may be null */
    protected boolean nullValue;
    protected boolean withSumFunc;
    protected boolean withIsNull;
    protected boolean withSubQuery;
    protected boolean correlatedSubQuery;
    protected boolean withUnValAble;
    protected boolean fixed;
    protected ItemResult cmpContext;
    /* the default charset index is my_charset_bin */
    protected int charsetIndex = 63;
    private HashSet<PlanNode> referTables;

    public boolean fixFields() {
        // We do not check fields which are fixed during construction
        assert (!fixed || basicConstItem());
        fixed = true;
        return false;
    }

    public abstract Item fixFields(NameResolutionContext context);

    public ItemResult resultType() {
        return ItemResult.REAL_RESULT;
    }

    public List<Item> arguments() {
        return null;
    }

    public int getArgCount() {
        return 0;
    }

    /**
     * Result type when an item appear in a numeric context. See
     * Field::numeric_context_result_type() for more comments.
     */
    public ItemResult numericContextResultType() {
        if (isTemporal())
            return decimals != 0 ? ItemResult.DECIMAL_RESULT : ItemResult.INT_RESULT;
        if (resultType() == ItemResult.STRING_RESULT)
            return ItemResult.REAL_RESULT;
        return resultType();
    }

    /**
     * Similar to result_type() but makes DATE, DATETIME, TIMESTAMP pretend to
     * be numbers rather than strings.
     */
    public ItemResult temporalWithDateAsNumberResultType() {
        return isTemporalWithDate() ? (decimals == 0 ? ItemResult.DECIMAL_RESULT : ItemResult.INT_RESULT) : resultType();
    }

    public ItemResult castToIntType() {
        return resultType();
    }

    public FieldTypes stringFieldType() {
        FieldTypes fType = FieldTypes.MYSQL_TYPE_VAR_STRING;
        if (maxLength >= 16777216)
            fType = FieldTypes.MYSQL_TYPE_LONG_BLOB;
        else if (maxLength >= 65536)
            fType = FieldTypes.MYSQL_TYPE_MEDIUM_BLOB;
        return fType;
    }

    public FieldTypes fieldType() {
        ItemResult i = resultType();
        if (i == ItemResult.STRING_RESULT) {
            return stringFieldType();
        } else if (i == ItemResult.INT_RESULT) {
            return FieldTypes.MYSQL_TYPE_LONGLONG;
        } else if (i == ItemResult.DECIMAL_RESULT) {
            return FieldTypes.MYSQL_TYPE_NEWDECIMAL;
        } else if (i == ItemResult.REAL_RESULT) {
            return FieldTypes.MYSQL_TYPE_DOUBLE;
        } else {
            return FieldTypes.MYSQL_TYPE_VARCHAR;
        }
    }

    public abstract ItemType type();

    /**
     * the difference of val_real and val_decimal is;
     * val_real return BigDecimal.zero if need return null
     *
     * @return
     */
    public abstract BigDecimal valReal();

    /**
     * will not return null,return BigInteger.zero if need null
     *
     * @return
     */
    public abstract BigInteger valInt();

    /**
     * Return date value of item in packed longlong format.
     */
    public long valDateTemporal() {
        MySQLTime ltime = new MySQLTime();
        if (nullValue = getDate(ltime, 1))
            return 0;
        return MyTime.timeToLonglongDatetimePacked(ltime);
    }

    /**
     * Return time value of item in packed longlong format.
     */
    public long valTimeTemporal() {
        MySQLTime ltime = new MySQLTime();
        if (nullValue = getTime(ltime))
            return 0;
        return MyTime.timeToLonglongTimePacked(ltime);
    }

    public long valTemporalByFieldType() {
        if (fieldType() == FieldTypes.MYSQL_TYPE_TIME)
            return valTimeTemporal();
        assert (isTemporalWithDate());
        return valDateTemporal();
    }

    public abstract String valStr();

    public abstract BigDecimal valDecimal();

    /**
     * ProtocolText::ResultsetRow:
     * <p>
     * A row with the data for each column.
     * <p>
     * NULL is sent as 0xfb
     * <p>
     * everything else is converted into a string and is sent as
     * Protocol::LengthEncodedString.
     *
     * @return
     */
    public byte[] getRowPacketByte() {
        byte[] result = null;
        FieldTypes fType;

        FieldTypes i = fType = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_TINY || i == FieldTypes.MYSQL_TYPE_SHORT || i == FieldTypes.MYSQL_TYPE_YEAR || i == FieldTypes.MYSQL_TYPE_INT24 || i == FieldTypes.MYSQL_TYPE_LONG || i == FieldTypes.MYSQL_TYPE_LONGLONG) {
            BigInteger bi = valInt();
            if (!nullValue)
                result = bi.toString().getBytes();
        } else if (i == FieldTypes.MYSQL_TYPE_FLOAT || i == FieldTypes.MYSQL_TYPE_DOUBLE) {
            BigDecimal bd = valReal();
            if (!nullValue)
                result = bd.toString().getBytes();
        } else if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            MySQLTime tm = new MySQLTime();
            getDate(tm, MyTime.TIME_FUZZY_DATE);
            if (!nullValue) {
                if (fType == FieldTypes.MYSQL_TYPE_DATE) {
                    result = MyTime.myDateToStr(tm).getBytes();
                } else {
                    result = MyTime.myDatetimeToStr(tm, decimals).getBytes();
                }
            }
        } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
            MySQLTime tm = new MySQLTime();
            getTime(tm);
            if (!nullValue)
                result = MyTime.myTimeToStrL(tm, decimals).getBytes();
        } else {
            String res = null;
            if ((res = valStr()) != null)
                try {
                    result = res.getBytes(charset());
                } catch (UnsupportedEncodingException e) {
                    LOGGER.info(e.toString());
                }
            else {
                assert (nullValue);
                BigInteger bi = valInt();
                if (!nullValue)
                    result = bi.toString().getBytes();
            }
        }
        if (nullValue)
            result = null;
        return result;
    }

    public Item getResultItem() {
        FieldTypes fType;

        FieldTypes i = fType = fieldType();
        if (i == FieldTypes.MYSQL_TYPE_TINY || i == FieldTypes.MYSQL_TYPE_SHORT || i == FieldTypes.MYSQL_TYPE_YEAR || i == FieldTypes.MYSQL_TYPE_INT24 || i == FieldTypes.MYSQL_TYPE_LONG || i == FieldTypes.MYSQL_TYPE_LONGLONG) {
            BigInteger bi = valInt();
            if (!nullValue) {
                return new ItemInt(bi.longValue());
            }
        } else if (i == FieldTypes.MYSQL_TYPE_FLOAT || i == FieldTypes.MYSQL_TYPE_DOUBLE) {
            BigDecimal bd = valReal();
            if (!nullValue) {
                return new ItemFloat(bd);
            }
        } else if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            MySQLTime tm = new MySQLTime();
            getDate(tm, MyTime.TIME_FUZZY_DATE);
            if (!nullValue) {
                if (fType == FieldTypes.MYSQL_TYPE_DATE) {
                    return new ItemString(MyTime.myDateToStr(tm), this.charsetIndex);
                } else {
                    return new ItemString(MyTime.myDatetimeToStr(tm, decimals), this.charsetIndex);
                }
            }
        } else if (i == FieldTypes.MYSQL_TYPE_TIME) {
            MySQLTime tm = new MySQLTime();
            getTime(tm);
            if (!nullValue)
                return new ItemString(MyTime.myTimeToStrL(tm, decimals), this.charsetIndex);
        } else {
            String res;
            if ((res = valStr()) != null) {
                return new ItemString(res, this.charsetIndex);
            } else {
                assert (nullValue);
                BigInteger bi = valInt();
                if (!nullValue) {
                    return new ItemInt(bi.longValue());
                }
            }
        }
        return null;
    }

    public boolean valBool() {
        ItemResult i = resultType();
        if (i == ItemResult.INT_RESULT) {
            return valInt().longValue() != 0;
        } else if (i == ItemResult.DECIMAL_RESULT) {
            BigDecimal val = valDecimal();
            if (val != null)
                return val.compareTo(BigDecimal.ZERO) != 0;
            return false;
        } else if (i == ItemResult.REAL_RESULT || i == ItemResult.STRING_RESULT) {
            return !(valReal().compareTo(BigDecimal.ZERO) == 0);
        } else {
            return false; // Wrong (but safe)
        }
    }
    /*
     * Returns true if this is a simple constant item like an integer, not a
     * constant expression. Used in the optimizer to propagate basic constants.
     */

    public boolean basicConstItem() {
        return false;
    }

    public int maxCharLength() {
        return maxLength / 1;
    }

    public int floatLength(int decimalsPar) {
        return decimals != NOT_FIXED_DEC ? (MySQLcom.DBL_DIG + 2 + decimalsPar) : MySQLcom.DBL_DIG + 8;
    }

    /**
     * @return
     */
    public int decimalPrecision() {
        // TODO
        return MySQLcom.DBL_DIG + 10;
    }

    public final int decimalIntPart() {
        return decimalPrecision() - decimals;
    }

    public Comparable getValueWithType(ItemResult type) {
        if (type == ItemResult.REAL_RESULT) {
            return valReal();
        } else if (type == ItemResult.DECIMAL_RESULT) {
            return valDecimal();
        } else if (type == ItemResult.INT_RESULT) {
            return valInt();
        } else if (type == ItemResult.STRING_RESULT) {
            return valStr();
        }
        return null;
    }

    public boolean isTemporalWithDate() {
        return FieldUtil.isTemporalTypeWithDate(fieldType());
    }

    public boolean isTemporalWithDateAndTime() {
        return FieldUtil.isTemporalTypeWithDateAndTime(fieldType());
    }

    public boolean isTemporalWithTime() {
        return FieldUtil.isTemporalTypeWithTime(fieldType());
    }

    public boolean isTemporal() {
        return FieldUtil.isTemporalType(fieldType());
    }

    public abstract boolean getDate(MySQLTime ltime, long fuzzydate);

    public abstract boolean getTime(MySQLTime ltime);

    public boolean isNull() {
        return false;
    }
    /*
     * Make sure the null_value member has a correct value.
     */

    public void updateNullValue() {
        valInt();
    }

    public void fixCharLength(int maxCharLength) {
        maxLength = maxCharLength;
    }

    public void makeField(FieldPacket tmpFp) {
        initMakeField(tmpFp, fieldType());
    }

    /*
     * This implementation can lose str_value content, so if the Item uses
     * str_value to store something, it should reimplement it's
     * ::save_in_field() as Item_string, for example, does.
     *
     * Note: all Item_XXX::val_str(str) methods must NOT rely on the fact that
     * str != str_value. For example, see fix for bug #44743.
     */

    /**
     * Save a temporal value in packed longlong format into a Field. Used in
     * optimizer.
     *
     * @param[out] field The field to set the value to.
     * @retval 0 On success.
     * @retval >0 In error.
     */
    public TypeConversionStatus saveInField(Field field, boolean noConversions) {
        try {
            if (resultType() == ItemResult.STRING_RESULT) {
                String result = valStr();
                if (nullValue) {
                    field.setPtr(null);
                    return TypeConversionStatus.TYPE_OK;
                }
                field.setPtr(result.getBytes(charset()));
            } else if (resultType() == ItemResult.REAL_RESULT && field.resultType() == ItemResult.STRING_RESULT) {
                BigDecimal nr = valReal();
                if (nullValue) {
                    field.setPtr(null);
                    return TypeConversionStatus.TYPE_OK;
                }
                field.setPtr(nr.toString().getBytes());
            } else if (resultType() == ItemResult.REAL_RESULT) {
                BigDecimal nr = valReal();
                if (nullValue) {
                    field.setPtr(null);
                    return TypeConversionStatus.TYPE_OK;
                }
                field.setPtr(nr.toString().getBytes());
            } else if (resultType() == ItemResult.DECIMAL_RESULT) {
                BigDecimal value = valDecimal();
                if (nullValue) {
                    field.setPtr(null);
                    return TypeConversionStatus.TYPE_OK;
                }
                field.setPtr(value.toString().getBytes());
            } else {
                BigInteger nr = valInt();
                if (nullValue) {
                    field.setPtr(null);
                    return TypeConversionStatus.TYPE_OK;
                }
                field.setPtr(nr.toString().getBytes());
            }
            return TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        } catch (Exception e) {
            return TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        }
    }
    /*-------------------------helper funtion----------------------------*/

    protected String valStringFromReal() {
        BigDecimal nr = valReal();
        if (nullValue)
            return null; /* purecov: inspected */
        return nr.toString();
    }

    protected String valStringFromInt() {
        return valInt().toString();
    }

    protected String valStringFromDecimal() {
        BigDecimal bd = valDecimal();
        if (nullValue)
            return null; /* purecov: inspected */
        if (bd == null)
            return null;
        return bd.toString();
    }

    protected String valStringFromDate() {
        MySQLTime ltime = new MySQLTime();
        if (getDate(ltime, 1))
            return null;
        return MyTime.myDateToStr(ltime);
    }

    protected String valStringFromTime() {
        MySQLTime ltime = new MySQLTime();
        if (getTime(ltime))
            return null;
        return MyTime.myTimeToStrL(ltime, this.decimals);
    }

    protected String valStringFromDatetime() {
        MySQLTime ltime = new MySQLTime();
        if (getDate(ltime, 1))
            return null;
        return MyTime.myDatetimeToStr(ltime, this.decimals);
    }

    protected BigDecimal valDecimalFromReal() {
        BigDecimal nr = valReal();
        if (nullValue)
            return null; /* purecov: inspected */
        return nr;
    }

    protected BigDecimal valDecimalFromInt() {
        BigInteger nr = valInt();
        return new BigDecimal(nr);
    }

    protected BigDecimal valDecimalFromString() {
        String res = valStr();
        if (res == null)
            return null;
        try {
            return new BigDecimal(res);
        } catch (NumberFormatException ne) {
            return null;
        }
    }

    protected BigDecimal valDecimalFromDate() {
        MySQLTime ltime = new MySQLTime();
        return getDate(ltime, 1) ? null : MyTime.date2MyDecimal(ltime);
    }

    protected BigDecimal valDecimalFromTime() {
        MySQLTime ltime = new MySQLTime();
        return getTime(ltime) ? null : MyTime.time2MyDecimal(ltime);
    }

    protected long valIntFromDecimal() {
        BigDecimal bd = valDecimal();
        if (nullValue)
            return 0;
        return bd.longValue();
    }

    protected long valIntFromDate() {
        MySQLTime ltime = new MySQLTime();
        return getDate(ltime, 1) ? 0L : MyTime.timeToUlonglongDate(ltime);
    }

    protected long valIntFromDatetime() {
        MySQLTime ltime = new MySQLTime();
        return getDate(ltime, 1) ? 0L : MyTime.timeToUlonglongDatetimeRound(ltime);
    }

    protected long valIntFromTime() {
        MySQLTime ltime = new MySQLTime();
        return getTime(ltime) ? 0L : MyTime.timeToUlonglongTimeRound(ltime);
    }

    protected BigDecimal valRealFromDecimal() {
        /* Note that fix_fields may not be called for Item_avg_field items */
        BigDecimal decVal = valDecimal();
        if (nullValue)
            return BigDecimal.ZERO;
        return decVal;
    }

    protected boolean getDateFromString(MySQLTime ltime, long flags) {
        String res = null;
        if ((res = valStr()) == null) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            return true;
        }
        MySQLTimeStatus status = new MySQLTimeStatus();
        return MyTime.strToDatetime(res, res.length(), ltime, flags, status);
    }

    protected boolean getDateFromReal(MySQLTime ltime, long flags) {
        double value = valReal().doubleValue();
        if (nullValue) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            return true;
        }
        return MyTime.myDoubleToDatetimeWithWarn(value, ltime, flags);
    }

    protected boolean getDateFromDecimal(MySQLTime ltime, long flags) {
        BigDecimal value = valDecimal();
        if (nullValue) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            return true;
        }
        return MyTime.myDecimalToDatetimeWithWarn(value, ltime, flags);
    }

    protected boolean getDateFromInt(MySQLTime ltime, long flags) {
        long value = valInt().longValue();
        if (nullValue) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            return true;
        }
        MyTime.timeFromLonglongDatetimePacked(ltime, value);
        return false;
    }

    protected boolean getDateFromTime(MySQLTime ltime) {
        MySQLTime tmp = new MySQLTime();
        if (getTime(tmp)) {
            assert (nullValue);
            return true;
        }
        MyTime.timeToDatetime(tmp, ltime);
        return false;
    }

    /**
     * Convert a numeric type to date
     */
    protected boolean getDateFromNumeric(MySQLTime ltime, long flags) {
        ItemResult i = resultType();
        if (i == ItemResult.REAL_RESULT) {
            return getDateFromReal(ltime, flags);
        } else if (i == ItemResult.DECIMAL_RESULT) {
            return getDateFromDecimal(ltime, flags);
        } else if (i == ItemResult.INT_RESULT) {
            return getDateFromInt(ltime, flags);
        } else assert i != ItemResult.STRING_RESULT && i != ItemResult.ROW_RESULT;
        return (nullValue = true); // Impossible result_type
    }

    /**
     * Convert a non-temporal type to date
     */
    protected boolean getDateFromNonTemporal(MySQLTime ltime, long fuzzydate) {
        assert (!isTemporal());
        ItemResult i = resultType();
        if (i == ItemResult.STRING_RESULT) {
            return getDateFromString(ltime, fuzzydate);
        } else if (i == ItemResult.REAL_RESULT) {
            return getDateFromReal(ltime, fuzzydate);
        } else if (i == ItemResult.DECIMAL_RESULT) {
            return getDateFromDecimal(ltime, fuzzydate);
        } else if (i == ItemResult.INT_RESULT) {
            return getDateFromInt(ltime, fuzzydate);
        } else assert i != ItemResult.ROW_RESULT;
        return (nullValue = true); // Impossible result_type
    }

    /**
     * Convert val_str() to time in MYSQL_TIME
     */
    protected boolean getTimeFromString(MySQLTime ltime) {
        String res = valStr();
        if (res == null) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
            return true;
        }
        return MyTime.strToTime(res, res.length(), ltime, new MySQLTimeStatus());
    }

    /**
     * Convert val_real() to time in MYSQL_TIME
     */
    protected boolean getTimeFromReal(MySQLTime ltime) {
        double value = valReal().doubleValue();
        if (nullValue) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
            return true;
        }
        return MyTime.myDoubleToTimeWithWarn(value, ltime);
    }

    /**
     * Convert val_decimal() to time in MYSQL_TIME
     */
    protected boolean getTimeFromDecimal(MySQLTime ltime) {
        BigDecimal decimal = valDecimal();
        if (nullValue) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
            return true;
        }
        return MyTime.myDecimalToTimeWithWarn(decimal, ltime);
    }

    /**
     * Convert val_int() to time in MYSQL_TIME
     */
    protected boolean getTimeFromInt(MySQLTime ltime) {
        long value = valInt().longValue();
        if (nullValue) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
            return true;
        }
        MyTime.timeFromLonglongTimePacked(ltime, value);
        return false;
    }

    /**
     * Convert date to time
     */
    protected boolean getTimeFromDate(MySQLTime ltime) {
        if (getDate(ltime, 1)) // Need this check if NULL value
            return true;
        ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        return false;
    }

    /**
     * Convert datetime to time
     */
    protected boolean getTimeFromDatetime(MySQLTime ltime) {
        if (getDate(ltime, 1))
            return true;
        MyTime.datetimeToTime(ltime);
        return false;
    }

    /**
     * Convert a numeric type to time
     */
    protected boolean getTimeFromNumeric(MySQLTime ltime) {
        assert (!isTemporal());
        ItemResult i = resultType();
        if (i == ItemResult.REAL_RESULT) {
            return getTimeFromReal(ltime);
        } else if (i == ItemResult.DECIMAL_RESULT) {
            return getTimeFromDecimal(ltime);
        } else if (i == ItemResult.INT_RESULT) {
            return getTimeFromInt(ltime);
        } else assert i != ItemResult.STRING_RESULT && i != ItemResult.ROW_RESULT;
        return (nullValue = true); // Impossible result type
    }

    /**
     * Convert a non-temporal type to time
     */
    protected boolean getTimeFromNonTemporal(MySQLTime ltime) {
        assert (!isTemporal());
        ItemResult i = resultType();
        if (i == ItemResult.STRING_RESULT) {
            return getTimeFromString(ltime);
        } else if (i == ItemResult.REAL_RESULT) {
            return getTimeFromReal(ltime);
        } else if (i == ItemResult.DECIMAL_RESULT) {
            return getTimeFromDecimal(ltime);
        } else if (i == ItemResult.INT_RESULT) {
            return getTimeFromInt(ltime);
        } else assert i != ItemResult.ROW_RESULT;
        return (nullValue = true); // Impossible result type
    }

    /*-----------------------------end helper functions----------------*/

    /**
     * -- not so important functions --
     **/
    public void fixLengthAndDecAndCharsetDatetime(int maxCharLengthArg, int decArg) {
        decimals = decArg;
        fixCharLength(maxCharLengthArg + (decArg != 0 ? decArg + 1 : 0));
    }

    public int datetimePrecision() {
        if (resultType() == ItemResult.STRING_RESULT && !isTemporal()) {
            MySQLTime ltime = new MySQLTime();
            String tmp = valStr();
            MySQLTimeStatus status = new MySQLTimeStatus();
            // Nanosecond rounding is not needed, for performance purposes
            if ((tmp != null) && !MyTime.strToDatetime(tmp, tmp.length(), ltime,
                    MyTime.TIME_NO_NSEC_ROUNDING | MyTime.TIME_FUZZY_DATE, status))
                return Math.min((int) status.getFractionalDigits(), MyTime.DATETIME_MAX_DECIMALS);
        }
        return Math.min(decimals, MyTime.DATETIME_MAX_DECIMALS);
    }
    /*
     * - Return NULL if argument is NULL. - Return zero if argument is not NULL,
     * but we could not convert it to DATETIME. - Return zero if argument is not
     * NULL and represents a valid DATETIME value, but the value is out of the
     * supported Unix timestamp range.
     */

    public boolean getTimeval(Timeval tm) {
        MySQLTime ltime = new MySQLTime();
        if (getDate(ltime, MyTime.TIME_FUZZY_DATE)) {
            if (nullValue)
                return true; /* Value is NULL */
            else {
                tm.setTvUsec(0);
                tm.setTvSec(0);
                return false;
            }
        }
        if (MyTime.datetimeToTimeval(ltime, tm)) {
            /*
             * Value is out of the supported range
             */
            tm.setTvUsec(0);
            tm.setTvSec(0);
            return false;
        }
        return false; /* Value is a good Unix timestamp */
    }

    private void initMakeField(FieldPacket tmpField, FieldTypes fieldType) {
        byte[] emptyName = new byte[]{};
        tmpField.setDb(emptyName);
        tmpField.setOrgTable(emptyName);
        tmpField.setOrgName(emptyName);
        tmpField.setCharsetIndex(charsetIndex);
        try {
            tmpField.setName((getAlias() == null ? getItemName() : getAlias()).getBytes(charset()));
        } catch (UnsupportedEncodingException e) {
            LOGGER.info("parse string exception!", e);
        }
        tmpField.setFlags((maybeNull ? 0 : FieldUtil.NOT_NULL_FLAG));
        tmpField.setType(fieldType.numberValue());
        tmpField.setLength(maxLength);
        tmpField.setDecimals((byte) decimals);
    }

    private String charset() {
        return CharsetUtil.getJavaCharset(charsetIndex);
    }

    /**
     * @return
     */
    public int timePrecision() {
        if (canValued() && resultType() == ItemResult.STRING_RESULT && !isTemporal()) {
            MySQLTime ltime = new MySQLTime();
            String tmp = valStr();
            MySQLTimeStatus status = new MySQLTimeStatus();
            // Nanosecond rounding is not needed, for performance purposes
            if (tmp != null && !MyTime.strToTime(tmp, tmp.length(), ltime, status))
                return Math.min((int) status.getFractionalDigits(), MyTime.DATETIME_MAX_DECIMALS);
        }
        return Math.min(decimals, MyTime.DATETIME_MAX_DECIMALS);
    }

    public boolean isWild() {
        return false;
    }

    public final String getAlias() {
        if (this.aliasName == null) {
            if (itemName != null && ((itemName.charAt(0) == '\'') || itemName.charAt(0) == '"')) {
                setAlias(this.itemName);
            }
        }
        return this.aliasName;
    }

    public final void setAlias(String alias) {
        if (alias != null && ((alias.charAt(0) == '\'') || alias.charAt(0) == '"')) {
            alias = alias.substring(1, alias.length() - 1);
        }
        this.aliasName = alias;
    }

    public String getPushDownName() {
        return pushDownName;
    }

    public void setPushDownName(String pushDownName) {
        this.pushDownName = pushDownName;
    }
    //TODO:YHQ  NEED CHECK

    public final String getItemName() {
        if (itemName == null || itemName.length() == 0) {
            SQLExpr expr = toExpression();
            StringBuilder sb = new StringBuilder();
            MySqlOutputVisitor ov = new MySqlOutputVisitor(sb);
            ov.setShardingSupport(false);
            expr.accept(ov);
            itemName = sb.toString();
        }
        return itemName;
    }

    public final void setItemName(String itemName) {
        this.itemName = itemName;
    }

    /**
     * clone item include alias
     *
     * @return
     */
    public final Item cloneItem() {
        Item cloneItem = cloneStruct(false, null, false, null);
        cloneItem.itemName = itemName;
        cloneItem.aliasName = aliasName;
        return cloneItem;
    }

    public final HashSet<PlanNode> getReferTables() {
        // so if we don't use this method all the time, refertables is null
        if (referTables == null)
            referTables = new HashSet<>(2);
        return referTables;
    }

    public final boolean canValued() {
        if (withUnValAble)
            return false;
        else {
            fixFields();
            return true;
        }
    }

    public String getTableName() {
        return null;
    }

    public String getDbName() {
        return null;
    }

    /**
     * added to construct all refers in an item
     *
     * @param context
     */
    public abstract void fixRefer(ReferContext context);

    public abstract SQLExpr toExpression();

    public final Item cloneStruct() {
        Item clone = cloneStruct(false, null, false, null);
        clone.withSumFunc = withSumFunc;
        clone.withIsNull = withIsNull;
        clone.withSubQuery = withSubQuery;
        clone.correlatedSubQuery = correlatedSubQuery;
        clone.withUnValAble = withUnValAble;
        clone.pushDownName = pushDownName;
        clone.getReferTables().addAll(getReferTables());
        return clone;
    }

    /**
     * cloen item's struct,visitName is all empty
     *
     * @param forCalculate if true,then use arguments after;else,only for name setup
     * @param calArgs      used in queryHandler calculate
     * @param isPushDown
     * @param fields
     * @return
     */
    protected abstract Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown,
                                        List<Field> fields);

    public final Item reStruct(List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return cloneStruct(true, calArgs, isPushDown, fields);
    }

    //TODO:YHQ  NEED CHECK
    protected final List<SQLExpr> toExpressionList(List<Item> args) {
        if (args == null)
            return null;
        List<SQLExpr> newList = new ArrayList<>();
        for (Item item : args) {
            newList.add(item.toExpression());
        }
        return newList;
    }

    protected final List<Item> cloneStructList(List<Item> args) {
        if (args == null)
            return null;
        List<Item> newList = new ArrayList<>();
        for (Item toClone : args) {
            newList.add(toClone.cloneStruct());
        }
        return newList;
    }

    @Override
    public String toString() {
        return getItemName();
    }

}
