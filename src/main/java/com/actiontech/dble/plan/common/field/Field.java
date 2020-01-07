/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.num.*;
import com.actiontech.dble.plan.common.field.string.*;
import com.actiontech.dble.plan.common.field.temporal.*;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class Field {
    public static Field getFieldItem(byte[] name, byte[] db, byte[] table, byte[] orgTable, int type, int charsetIndex, int fieldLength,
                                     int decimals, long flags) {
        String charset = CharsetUtil.getJavaCharset(charsetIndex);
        try {
            return getFieldItem(new String(name, charset),
                    (db == null || db.length == 0) ? null : new String(db, charset),
                    (table == null || table.length == 0) ? null : new String(table, charset),
                    (orgTable == null || orgTable.length == 0) ? null : new String(orgTable, charset),
                    type, charsetIndex, fieldLength, decimals, flags);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("parser error ,charset :" + charset);
        }
    }

    public static Field getFieldItem(String name, String dbName, String table, String orgTable, int type, int charsetIndex, int fieldLength,
                                     int decimals, long flags) {
        FieldTypes fieldType = FieldTypes.valueOf(type);
        switch (fieldType) {
            case MYSQL_TYPE_JSON:
                return new FieldJson(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_NEWDECIMAL:  // mysql use newdecimal after some version
                return new FieldNewdecimal(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_DECIMAL:
                return new FieldDecimal(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_TINY:
                return new FieldTiny(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_SHORT:
                return new FieldShort(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_LONG:
                return new FieldLong(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_FLOAT:
                return new FieldFloat(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_DOUBLE:
                return new FieldDouble(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_NULL:
                return FieldNull.getInstance();
            case MYSQL_TYPE_TIMESTAMP:
                return new FieldTimestamp(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_LONGLONG:
                return new FieldLonglong(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_INT24:
                return new FieldMedium(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_NEWDATE:
                return new FieldDate(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_TIME:
                return new FieldTime(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_DATETIME:
                return new FieldDatetime(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_YEAR:
                return new FieldYear(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_VARCHAR:
                return new FieldVarchar(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_BIT:
                return new FieldBit(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_VAR_STRING:
                return new FieldVarstring(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_STRING:
                return new FieldString(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            /** --not support below, because select * change to string, can't get the origin type-- **/
            case MYSQL_TYPE_ENUM:
                return new FieldEnum(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_SET:
                return new FieldSet(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            case MYSQL_TYPE_TINY_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_BLOB:
                return new FieldBlob(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
            default:
                throw new RuntimeException("unsupported field type :" + fieldType.toString() + "!");
        }
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(Field.class);

    protected String name;
    protected String table;
    protected String orgTable;
    protected String dbName;
    protected int charsetIndex;
    protected String javaCharsetName;
    protected long flags;
    protected byte[] ptr;
    protected int fieldLength;
    protected int decimals;

    public Field(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        this.name = name;
        this.dbName = dbName;
        this.table = table;
        this.orgTable = orgTable;
        this.charsetIndex = charsetIndex;
        this.fieldLength = fieldLength;
        this.flags = flags;
        this.decimals = decimals;
        this.javaCharsetName = CharsetUtil.getJavaCharset(charsetIndex);
    }

    public abstract Item.ItemResult resultType();

    public Item.ItemResult numericContextResultType() {
        return resultType();
    }

    public abstract FieldTypes fieldType();

    public Item.ItemResult cmpType() {
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
            val = MySQLcom.getFullString(javaCharsetName, ptr);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.info("parse string exception!", ue);
        }
        return val;
    }

    /**
     * maybeNull
     *
     * @return
     */
    public boolean maybeNull() {
        return (FieldUtil.NOT_NULL_FLAG & flags) == 0;
    }

    public void makeField(FieldPacket fp) {
        try {
            fp.setName(this.name.getBytes(javaCharsetName));
            fp.setDb(this.dbName != null ? this.dbName.getBytes(javaCharsetName) : null);
            fp.setTable(this.table != null ? this.table.getBytes(javaCharsetName) : null);
            fp.setOrgTable(this.orgTable != null ? this.orgTable.getBytes(javaCharsetName) : null);
        } catch (UnsupportedEncodingException ue) {
            LOGGER.info("parse string exception!", ue);
        }
        fp.setCharsetIndex(this.charsetIndex);
        fp.setLength(this.fieldLength);
        fp.setFlags((int) this.flags);
        fp.setDecimals((byte) this.decimals);
        fp.setType(fieldType().numberValue());
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
     * get inner value
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

    /**
     * -- the length of field --
     **/
    public String getName() {
        return name;
    }

    public String getTable() {
        return table;
    }

    public String getOrgTable() {
        return orgTable;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    public String getJavaCharsetName() {
        return javaCharsetName;
    }

    public void setJavaCharsetName(String javaCharsetName) {
        this.javaCharsetName = javaCharsetName;
    }

    public long getFlags() {
        return flags;
    }

    public void setFlags(long flags) {
        this.flags = flags;
    }

    public byte[] getPtr() {
        return ptr;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }
}
