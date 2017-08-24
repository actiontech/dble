package io.mycat.plan.common.field;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.num.*;
import io.mycat.plan.common.field.string.*;
import io.mycat.plan.common.field.temporal.*;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item.ItemResult;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class Field {
    public static Field getFieldItem(byte[] name, byte[] table, int type, int charsetIndex, int fieldLength,
                                     int decimals, long flags) {
        String charset = CharsetUtil.getJavaCharset(charsetIndex);
        try {
            return getFieldItem(new String(name, charset),
                    (table == null || table.length == 0) ? null : new String(table, charset), type, charsetIndex,
                    fieldLength, decimals, flags);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("parser error ,charset :" + charset.toString());
        }
    }

    public static Field getFieldItem(String name, String table, int type, int charsetIndex, int fieldLength,
                                     int decimals, long flags) {
        FieldTypes fieldType = FieldTypes.valueOf(type);
        if (fieldType == FieldTypes.MYSQL_TYPE_NEWDECIMAL) { // mysql use newdecimal after some version
            return new FieldNewdecimal(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_DECIMAL) {
            return new FieldDecimal(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_TINY) {
            return new FieldTiny(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_SHORT) {
            return new FieldShort(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_LONG) {
            return new FieldLong(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_FLOAT) {
            return new FieldFloat(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_DOUBLE) {
            return new FieldDouble(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_NULL) {
            return FieldNull.getInstance();
        } else if (fieldType == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            return new FieldTimestamp(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_LONGLONG) {
            return new FieldLonglong(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_INT24) {
            return new FieldMedium(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_DATE || fieldType == FieldTypes.MYSQL_TYPE_NEWDATE) {
            return new FieldDate(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_TIME) {
            return new FieldTime(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_DATETIME) {
            return new FieldDatetime(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_YEAR) {
            return new FieldYear(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_VARCHAR) {
            return new FieldVarchar(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_BIT) {
            return new FieldBit(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_VAR_STRING) {
            return new FieldVarstring(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_STRING) {
            return new FieldString(name, table, charsetIndex, fieldLength, decimals, flags);
            /** --下列的类型函数目前不支持，因为select *出来的mysql都转化成string了，无法知晓它们在数据库中的type-- **/
        } else if (fieldType == FieldTypes.MYSQL_TYPE_ENUM) {
            return new FieldEnum(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_SET) {
            return new FieldSet(name, table, charsetIndex, fieldLength, decimals, flags);
        } else if (fieldType == FieldTypes.MYSQL_TYPE_TINY_BLOB || fieldType == FieldTypes.MYSQL_TYPE_MEDIUM_BLOB || fieldType == FieldTypes.MYSQL_TYPE_LONG_BLOB || fieldType == FieldTypes.MYSQL_TYPE_BLOB) {
            return new FieldBlob(name, table, charsetIndex, fieldLength, decimals, flags);
        } else {
            throw new RuntimeException("unsupported field type :" + fieldType.toString() + "!");
        }
    }

    protected static final Logger LOGGER = Logger.getLogger(Field.class);

    /**
     * -- field的长度 --
     **/
    public String name;
    public String table;
    public String dbname; // TODO
    public int charsetIndex;
    public String charsetName;
    public long flags;
    public byte[] ptr;
    public int fieldLength;
    public int decimals;

    public Field(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        this.name = name;
        this.table = table;
        this.charsetIndex = charsetIndex;
        this.fieldLength = fieldLength;
        this.flags = flags;
        this.decimals = decimals;
        this.charsetName = CharsetUtil.getJavaCharset(charsetIndex);
    }

    public abstract ItemResult resultType();

    public ItemResult numericContextResultType() {
        return resultType();
    }

    public abstract FieldTypes fieldType();

    public ItemResult cmpType() {
        return resultType();
    }

    public boolean isNull() {
        return ptr == null;
    }

    public void setPtr(byte[] ptr) {
        this.ptr = ptr;
    }

    public String valStr() {
        String val = null;
        try {
            val = MySQLcom.getFullString(charsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.warn("parse string exception!", ue);
        }
        return val;
    }

    /**
     * 是否有可能为空
     *
     * @return
     */
    public boolean maybeNull() {
        return (FieldUtil.NOT_NULL_FLAG & flags) == 0;
    }

    public void makeField(FieldPacket fp) {
        try {
            fp.name = this.name.getBytes(charsetName);
            fp.db = this.dbname != null ? this.dbname.getBytes(charsetName) : null;
        } catch (UnsupportedEncodingException ue) {
            LOGGER.warn("parse string exception!", ue);
        }
        fp.charsetIndex = this.charsetIndex;
        fp.length = this.fieldLength;
        fp.flags = (int) this.flags;
        fp.decimals = (byte) this.decimals;
        fp.type = fieldType().numberValue();
    }

    public abstract BigInteger valInt();

    public abstract BigDecimal valReal();

    public abstract BigDecimal valDecimal();

    public abstract int compareTo(Field other);

    public abstract int compare(byte[] v1, byte[] v2);

    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        String res = valStr();
        return res == null || MyTime.strToDatetimeWithWarn(res, ltime, fuzzydate);
    }

    public boolean getTime(MySQLTime ltime) {
        String res = valStr();
        return res == null || MyTime.strToTimeWithWarn(res, ltime);
    }

    /**
     * 计算出实际的对象的内部值
     */
    protected abstract void internalJob();

    public boolean equals(final Field other, boolean binaryCmp) {
        if (other == null)
            return false;
        if (this == other)
            return true;
        return this.compareTo(other) == 0;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    /**
     * Returns DATE/DATETIME value in packed longlong format. This method should
     * not be called for non-temporal types. Temporal field types override the
     * default method.
     */
    public long valDateTemporal() {
        assert (false);
        return 0;
    }

    /**
     * Returns TIME value in packed longlong format. This method should not be
     * called for non-temporal types. Temporal field types override the default
     * method.
     */
    public long valTimeTemporal() {
        assert (false);
        return 0;
    }

    @Override
    public int hashCode() {
        int h = 1;
        if (ptr != null) {
            for (int i = ptr.length - 1; i >= 0; i--)
                h = 31 * h + (int) ptr[i];
        }
        return h;
    }
}
