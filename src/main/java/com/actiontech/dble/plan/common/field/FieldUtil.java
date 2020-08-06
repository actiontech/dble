/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field;

import com.actiontech.dble.plan.common.item.FieldTypes;

import java.util.List;

public class FieldUtil {
    public static final int NOT_NULL_FLAG = 1; /* Field can't be NULL */
    public static final int PRI_KEY_FLAG = 2; /* Field is part of a primary key */
    public static final int UNIQUE_KEY_FLAG = 4; /* Field is part of a unique key */
    public static final int MULTIPLE_KEY_FLAG = 8; /* Field is part of a key */
    public static final int BLOB_FLAG = 16; /* Field is a blob */
    public static final int UNSIGNED_FLAG = 32; /* Field is unsigned */
    public static final int ZEROFILL_FLAG = 64; /* Field is zerofill */
    public static final int BINARY_FLAG = 128; /* Field is binary */

    /**
     * b1,b2 are int
     *
     * @param b1
     * @param b2
     * @return
     */
    public static int compareIntUsingStringBytes(byte[] b1, byte[] b2) {
        char b1c0 = (char) b1[0];
        char b2c0 = (char) b2[0];
        if (b1c0 == '-') { // b1<0
            if (b2c0 == '-') { // b2<0
                return -compareUnIntUsingStringBytes(b1, 1, b2, 1);
            } else {
                return -1;
            }
        } else { // b1>0
            if (b2c0 == '-') {
                return 1;
            } else {
                return compareUnIntUsingStringBytes(b1, 0, b2, 0);
            }
        }
    }

    /* b1,b2 are unsigned int*/
    private static int compareUnIntUsingStringBytes(byte[] b1, int startb1, byte[] b2, int startb2) {
        int b1len = b1.length - startb1;
        int b2len = b2.length - startb2;
        if (b1len < b2len)
            return -1;
        else if (b1len > b2len)
            return 1;
        else {
            // the length is equal
            for (int i = 0; i < b1len; i++) {
                byte bb1 = b1[startb1 + i];
                byte bb2 = b2[startb2 + i];
                if (bb1 > bb2)
                    return 1;
                else if (bb1 < bb2)
                    return -1;
                else
                    continue;
            }
            return 0;
        }
    }

    public int getEnumPackLength(int elements) {
        return elements < 256 ? 1 : 2;
    }

    public int getSetPackLength(int elements) {
        int len = (elements + 7) / 8;
        return len > 4 ? 8 : len;
    }

    public static void initFields(List<Field> fields, List<byte[]> bs) {
        int size = fields.size();
        for (int index = 0; index < size; index++) {
            fields.get(index).setPtr(bs.get(index));
        }
    }

    public static boolean isTemporalType(FieldTypes valuetype) {
        return valuetype == FieldTypes.MYSQL_TYPE_DATE || valuetype == FieldTypes.MYSQL_TYPE_DATETIME || valuetype == FieldTypes.MYSQL_TYPE_TIMESTAMP || valuetype == FieldTypes.MYSQL_TYPE_TIME || valuetype == FieldTypes.MYSQL_TYPE_NEWDATE;
    }

    /**
     * Tests if field real type is temporal, i.e. represents all existing
     * implementations of DATE, TIME, DATETIME or TIMESTAMP types in SQL.
     *
     * @param type Field real type, as returned by field->real_type()
     * @retval true If field real type is temporal
     * @retval false If field real type is not temporal
     */
    public static boolean isTemporalRealType(FieldTypes type) {
        if (type == FieldTypes.MYSQL_TYPE_TIME2 || type == FieldTypes.MYSQL_TYPE_TIMESTAMP2 || type == FieldTypes.MYSQL_TYPE_DATETIME2) {
            return true;
        } else {
            return FieldUtil.isTemporalType(type);
        }
    }

    public static boolean isTemporalTypeWithTime(FieldTypes type) {
        return type == FieldTypes.MYSQL_TYPE_TIME || type == FieldTypes.MYSQL_TYPE_DATETIME || type == FieldTypes.MYSQL_TYPE_TIMESTAMP;
    }

    public static boolean isTemporalTypeWithDate(FieldTypes valuetype) {
        return valuetype == FieldTypes.MYSQL_TYPE_DATE || valuetype == FieldTypes.MYSQL_TYPE_DATETIME || valuetype == FieldTypes.MYSQL_TYPE_TIMESTAMP;
    }

    /**
     * Tests if field type is temporal and has date and time parts, i.e.
     * represents DATETIME or TIMESTAMP types in SQL.
     *
     * @param type Field type, as returned by field->type().
     * @retval true If field type is temporal type with date and time parts.
     * @retval false If field type is not temporal type with date and time
     * parts.
     */
    public static boolean isTemporalTypeWithDateAndTime(FieldTypes type) {
        return type == FieldTypes.MYSQL_TYPE_DATETIME || type == FieldTypes.MYSQL_TYPE_TIMESTAMP;
    }

    /**
     * Recognizer for concrete data type (called real_type for some reason),
     * returning true if it is one of the TIMESTAMP types.
     */
    public static boolean isTimestampType(FieldTypes type) {
        return type == FieldTypes.MYSQL_TYPE_TIMESTAMP || type == FieldTypes.MYSQL_TYPE_TIMESTAMP2;
    }

    /**
     * Convert temporal real types as retuned by field->real_type() to field
     * type as returned by field->type().
     *
     * @param realType Real type.
     * @retval Field type.
     */
    public static FieldTypes realTypeToType(FieldTypes realType) {
        if (realType == FieldTypes.MYSQL_TYPE_TIME2) {
            return FieldTypes.MYSQL_TYPE_TIME;
        } else if (realType == FieldTypes.MYSQL_TYPE_DATETIME2) {
            return FieldTypes.MYSQL_TYPE_DATETIME;
        } else if (realType == FieldTypes.MYSQL_TYPE_TIMESTAMP2) {
            return FieldTypes.MYSQL_TYPE_TIMESTAMP;
        } else if (realType == FieldTypes.MYSQL_TYPE_NEWDATE) {
            return FieldTypes.MYSQL_TYPE_DATE;
        /* Note: NEWDECIMAL is a type, not only a real_type */
        } else {
            return realType;
        }
    }

    /*
     * Rules for merging different types of fields in UNION
     *
     * NOTE: to avoid 256*256 table, gap in table types numeration is skiped
     * following #defines describe that gap and how to canculate number of
     * fields and index of field in thia array.
     */
    private static final int FIELDTYPE_TEAR_FROM = (FieldTypes.MYSQL_TYPE_BIT.numberValue() + 1);
    private static final int FIELDTYPE_TEAR_TO = (FieldTypes.MYSQL_TYPE_JSON.numberValue() - 1);

    // private static int FIELDTYPE_NUM = (FIELDTYPE_TEAR_FROM + (255 -
    // FIELDTYPE_TEAR_TO));

    public static int fieldType2Index(FieldTypes fieldType) {
        fieldType = realTypeToType(fieldType);
        assert (fieldType.numberValue() < FIELDTYPE_TEAR_FROM || fieldType.numberValue() > FIELDTYPE_TEAR_TO);
        return (fieldType.numberValue() < FIELDTYPE_TEAR_FROM ? fieldType.numberValue() :
                FIELDTYPE_TEAR_FROM + (fieldType.numberValue() - FIELDTYPE_TEAR_TO) - 1);
    }

    public static FieldTypes fieldTypeMerge(FieldTypes a, FieldTypes b) {
        return FIELD_TYPES_MERGE_RULES[fieldType2Index(a)][fieldType2Index(b)];
    }

    private static final FieldTypes[][] FIELD_TYPES_MERGE_RULES = new FieldTypes[][]{
            /* enum_field_types.MYSQL_TYPE_DECIMAL -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_NEWDECIMAL,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_NEWDECIMAL,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_DECIMAL, FieldTypes.MYSQL_TYPE_DECIMAL,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_TINY -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_TINY,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_SHORT, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_TINY, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_INT24,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_SHORT -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_SHORT,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_SHORT, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_SHORT, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_INT24,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_SHORT,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_LONG -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_LONG, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_LONG, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_FLOAT -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_FLOAT,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_FLOAT,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_FLOAT,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_DOUBLE -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_NULL -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_TINY,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_SHORT, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_NULL, FieldTypes.MYSQL_TYPE_TIMESTAMP,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_LONGLONG,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_TIME,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_YEAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_BIT,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_JSON,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_ENUM,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_SET, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_GEOMETRY},
            /* enum_field_types.MYSQL_TYPE_TIMESTAMP -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_TIMESTAMP, FieldTypes.MYSQL_TYPE_TIMESTAMP,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_LONGLONG -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_LONGLONG,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_LONGLONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_LONGLONG,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_INT24 -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_INT24,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_INT24, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_INT24, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_INT24,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_INT24,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_DATE -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_TIME -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_TIME, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_TIME,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_DATETIME -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_YEAR -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_DECIMAL, FieldTypes.MYSQL_TYPE_TINY,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_SHORT, FieldTypes.MYSQL_TYPE_LONG,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_FLOAT, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_YEAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONGLONG, FieldTypes.MYSQL_TYPE_INT24,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_YEAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_NEWDATE -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_DATETIME,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_DATETIME, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_NEWDATE, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_VARCHAR -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_BIT -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_BIT, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_BIT,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* MYSQL_TYPE_JSON -> */
            {
                    //MYSQL_TYPE_DECIMAL      MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_SHORT        MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_FLOAT        FieldTypes.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_NULL         FieldTypes.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_JSON, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_LONGLONG     FieldTypes.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_DATE         FieldTypes.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_DATETIME     FieldTypes.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_NEWDATE      FieldTypes.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_BIT          <16>-<244>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_JSON,
                    //FieldTypes.MYSQL_TYPE_NEWDECIMAL   FieldTypes.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_SET          FieldTypes.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    //FieldTypes.MYSQL_TYPE_MEDIUM_BLOB  FieldTypes.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    //FieldTypes.MYSQL_TYPE_BLOB         FieldTypes.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    //FieldTypes.MYSQL_TYPE_STRING       FieldTypes.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_NEWDECIMAL -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_NEWDECIMAL,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_NEWDECIMAL,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_DOUBLE, FieldTypes.MYSQL_TYPE_DOUBLE,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_NEWDECIMAL,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_NEWDECIMAL,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_NEWDECIMAL, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_ENUM -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_ENUM, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_SET -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_SET, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_TINY_BLOB -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_TINY_BLOB, FieldTypes.MYSQL_TYPE_TINY_BLOB},
            /* enum_field_types.MYSQL_TYPE_MEDIUM_BLOB -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_MEDIUM_BLOB},
            /* enum_field_types.MYSQL_TYPE_LONG_BLOB -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_LONG_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB},
            /* enum_field_types.MYSQL_TYPE_BLOB -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_BLOB,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_BLOB},
            /* enum_field_types.MYSQL_TYPE_VAR_STRING -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR},
            /* enum_field_types.MYSQL_TYPE_STRING -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_STRING,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_STRING},
            /* enum_field_types.MYSQL_TYPE_GEOMETRY -> */
            {
                    // enum_field_types.MYSQL_TYPE_DECIMAL
                    // enum_field_types.MYSQL_TYPE_TINY
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SHORT
                    // enum_field_types.MYSQL_TYPE_LONG
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_FLOAT
                    // enum_field_types.MYSQL_TYPE_DOUBLE
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NULL
                    // enum_field_types.MYSQL_TYPE_TIMESTAMP
                    FieldTypes.MYSQL_TYPE_GEOMETRY, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_LONGLONG
                    // enum_field_types.MYSQL_TYPE_INT24
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATE
                    // enum_field_types.MYSQL_TYPE_TIME
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_DATETIME
                    // enum_field_types.MYSQL_TYPE_YEAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDATE
                    // enum_field_types.MYSQL_TYPE_VARCHAR
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_BIT <16>-<245>
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    //MYSQL_TYPE_JSON
                    FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_NEWDECIMAL
                    // enum_field_types.MYSQL_TYPE_ENUM
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_SET
                    // enum_field_types.MYSQL_TYPE_TINY_BLOB
                    FieldTypes.MYSQL_TYPE_VARCHAR, FieldTypes.MYSQL_TYPE_TINY_BLOB,
                    // enum_field_types.MYSQL_TYPE_MEDIUM_BLOB
                    // enum_field_types.MYSQL_TYPE_LONG_BLOB
                    FieldTypes.MYSQL_TYPE_MEDIUM_BLOB, FieldTypes.MYSQL_TYPE_LONG_BLOB,
                    // enum_field_types.MYSQL_TYPE_BLOB
                    // enum_field_types.MYSQL_TYPE_VAR_STRING
                    FieldTypes.MYSQL_TYPE_BLOB, FieldTypes.MYSQL_TYPE_VARCHAR,
                    // enum_field_types.MYSQL_TYPE_STRING
                    // enum_field_types.MYSQL_TYPE_GEOMETRY
                    FieldTypes.MYSQL_TYPE_STRING, FieldTypes.MYSQL_TYPE_GEOMETRY,
            },
    };


    public static boolean isNumberType(String dataType) {
        if (dataType == null) return false;
        dataType = dataType.toUpperCase();
        return dataType.contains("BIT") ||
                dataType.contains("TINYINT") ||
                dataType.contains("BOOL") ||
                dataType.contains("SMALLINT") ||
                dataType.contains("MEDIUMINT") ||
                dataType.contains("INT") ||
                dataType.contains("INTEGER") ||
                dataType.contains("BIGINT");
    }

}
