/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common;

import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemResult;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimeStatus;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public final class MySQLcom {
    private MySQLcom() {
    }

    public static final double M_PI = Math.PI;
    public static final int DBL_DIG = 6;
    public static final int FLT_DIG = 10;
    public static final int DECIMAL_LONGLONG_DIGITS = 22;

    /**
     * maximum length of buffer in our big digits (uint32).
     */
    public static final int DECIMAL_BUFF_LENGTH = 9;

    /* the number of digits that my_decimal can possibly contain */
    public static final int DECIMAL_MAX_POSSIBLE_PRECISION = (DECIMAL_BUFF_LENGTH * 9);

    /**
     * maximum guaranteed precision of number in decimal digits (number of our
     * digits * number of decimal digits in one our big digit - number of
     * decimal digits in one our big digit decreased by 1 (because we always put
     * decimal point on the border of our big digits))
     */
    public static final int DECIMAL_MAX_PRECISION = (DECIMAL_MAX_POSSIBLE_PRECISION - 8 * 2);

    public static final int DECIMAL_MAX_SCALE = 30;
    public static final int DECIMAL_NOT_SPECIFIED = 31;

    public static final BigInteger BI64BACK = new BigInteger("18446744073709551616");

    public static String setInt(long num, boolean unsignedFlag) {
        return String.valueOf(num);
    }

    public static String setReal(double num, int decimals) {
        BigDecimal bd = new BigDecimal(num);
        bd = bd.setScale(decimals);
        double db = bd.doubleValue();
        return String.valueOf(db);
    }

    public static BigDecimal int2Decimal(long num, boolean unsigned) {
        return BigDecimal.valueOf(num);
    }

    public static BigDecimal double2Decimal(double num, int decimals) {
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(decimals);
    }

    public static double str2Double(String str) {
        try {
            double db = Double.parseDouble(str);
            return db;
        } catch (Exception e) {
            return 0.0;
        }
    }

    public static long str2Long(String str) {
        try {
            long l = Long.parseLong(str);
            return l;
        } catch (Exception e) {
            return 0;
        }
    }

    public static BigDecimal str2Decimal(String str) {
        try {
            double db = Double.parseDouble(str);
            BigDecimal bd = BigDecimal.valueOf(db);
            return bd;
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    /**
     * str to longlong
     *
     * @param cs    str ->char array
     * @param start
     * @param end
     * @param error
     * @return
     */
    public static BigInteger myStrtoll10(char[] cs, int start, int end, BoolPtr error) {
        String tmp = new String(cs, start, end - start);
        error.set(false);
        try {
            return new BigInteger(tmp);
        } catch (Exception e) {
            error.set(true);
            return BigInteger.ZERO;
        }
    }

    /**
     * binary compare, num need to <= b1.size && <= b2.size
     *
     * @param b1
     * @param b2
     * @param num
     * @return
     */
    public static int memcmp(byte[] b1, byte[] b2, int num) {
        for (int i = 0; i < num; i++) {
            if (b1[i] < b2[i]) {
                return -1;
            } else if (b1[i] == b2[i]) {
                continue;
            } else {
                return 1;
            }
        }
        return 0;
    }

    public static int memcmp(byte[] aPtr, byte[] bPtr) {
        int aLen = aPtr.length, bLen = bPtr.length;
        if (aLen >= bLen)
            return memcmp0(aPtr, bPtr);
        else
            return -memcmp(bPtr, aPtr);
    }

    public static BigInteger getUnsignedLong(long l) {
        BigInteger bi = BigInteger.valueOf(l);
        BigInteger bmask = new BigInteger("FFFFFFFFFFFFFFFF", 16);
        return bi.and(bmask);
    }

    /**
     * @return converted value. 0 on error and on zero-dates -- check 'failure'
     * @brief Convert date provided in a string to its packed temporal int
     * representation.
     * @param[in] thd thread handle
     * @param[in] str a string to convert
     * @param[in] warn_type type of the timestamp for issuing the warning
     * @param[in] warn_name field name for issuing the warning
     * @param[out] error_arg could not extract a DATE or DATETIME
     * @details Convert date provided in the string str to the int
     * representation. If the string contains wrong date or doesn't
     * contain it at all then a warning is issued. The warn_type and
     * the warn_name arguments are used as the name and the type of the
     * field when issuing the warning.
     */
    public static long getDateFromStr(String str, MySQLTimestampType warntype, BoolPtr error) {
        MySQLTime ltime = new MySQLTime();
        MySQLTimeStatus status = new MySQLTimeStatus();
        error.set(MyTime.strToDatetime(str, str.length(), ltime, MyTime.TIME_FUZZY_DATE, status));
        if (error.get())
            return 0;
        return MyTime.timeToLonglongDatetimePacked(ltime);

    }
    /*
     * Collects different types for comparison of first item with each other
     * items
     *
     * SYNOPSIS collectCmpTypes() items Array of items to collect types from
     * nitems Number of items in the array skip_nulls Don't collect types of
     * NULL items if TRUE
     *
     * DESCRIPTION This function collects different result types for comparison
     * of the first item in the list with each of the remaining items in the
     * 'items' array.
     *
     * RETURN 0 - if row type incompatibility has been detected (see
     * cmp_row_type) Bitmap of collected types - otherwise show how many types there are
     */

    public static int collectCmpTypes(List<Item> items, boolean skipnulls) {
        int foundtypes = 0;
        ItemResult leftResult = items.get(0).resultType();
        for (int i = 1; i < items.size(); i++) {
            if (skipnulls && items.get(i).type() == Item.ItemType.NULL_ITEM)
                continue;
            if (leftResult == ItemResult.ROW_RESULT || items.get(i).resultType() == ItemResult.ROW_RESULT &&
                    cmpRowType(items.get(0), items.get(i)) != 0)
                return 0;
            foundtypes |= 1 << MySQLcom.itemCmpType(leftResult, items.get(i).resultType()).ordinal();
        }
        /*
         * Even if all right-hand items are NULLs and we are skipping them all,
         * we need at least one type bit in the found_type bitmask.
         */
        if (skipnulls && foundtypes == 0)
            foundtypes |= 1 << leftResult.ordinal();
        return foundtypes;
    }

    public static int cmpRowType(Item item1, Item item2) {
        // TODO
        return 0;
    }

    public static ItemResult itemCmpType(ItemResult a, ItemResult b) {

        if (a == ItemResult.STRING_RESULT && b == ItemResult.STRING_RESULT)
            return ItemResult.STRING_RESULT;
        if (a == ItemResult.INT_RESULT && b == ItemResult.INT_RESULT)
            return ItemResult.INT_RESULT;
        if ((a == ItemResult.INT_RESULT || a == ItemResult.DECIMAL_RESULT) &&
                (b == ItemResult.INT_RESULT || b == ItemResult.DECIMAL_RESULT))
            return ItemResult.DECIMAL_RESULT;
        return ItemResult.REAL_RESULT;
    }

    public static FieldTypes aggFieldType(List<Item> items, int startIndex, int nitems) {
        if (nitems == 0 || items.get(startIndex).resultType() == ItemResult.ROW_RESULT)
            return FieldTypes.valueOf("-1");
        FieldTypes res = items.get(startIndex).fieldType();
        for (int i = 1; i < nitems; i++)
            res = FieldUtil.fieldTypeMerge(res, items.get(startIndex + i).fieldType());
        return res;
    }

    public static ItemResult aggResultType(List<Item> items, int startIndex, int size) {
        ItemResult type = ItemResult.STRING_RESULT;
        /* Skip beginning NULL items */
        int index = 0, indexEnd;
        Item item;
        for (index = startIndex, indexEnd = startIndex + size; index < indexEnd; index++) {
            item = items.get(index);
            if (item.type() != ItemType.NULL_ITEM) {
                type = item.resultType();
                index++;
                break;
            }
        }
        /* Combine result types. Note: NULL items don't affect the result */
        for (; index < indexEnd; index++) {
            item = items.get(index);
            if (item.type() != ItemType.NULL_ITEM)
                type = itemStoreType(type, item);
        }
        return type;
    }

    public static ItemResult itemStoreType(ItemResult a, Item item) {
        ItemResult b = item.resultType();

        if (a == ItemResult.STRING_RESULT || b == ItemResult.STRING_RESULT)
            return ItemResult.STRING_RESULT;
        else if (a == ItemResult.REAL_RESULT || b == ItemResult.REAL_RESULT)
            return ItemResult.REAL_RESULT;
        else if (a == ItemResult.DECIMAL_RESULT || b == ItemResult.DECIMAL_RESULT)
            return ItemResult.DECIMAL_RESULT;
        else
            return ItemResult.INT_RESULT;
    }

    public static byte[] long2Byte(BigInteger bi) {
        long x = -1;
        if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)
            x = bi.subtract(BI64BACK).longValue();
        else
            x = bi.longValue();
        int retLen = -1;
        byte[] bb = new byte[8];
        int index = -1;
        bb[++index] = (byte) (x >> 56);
        if (bb[index] != 0)
            retLen = 8;
        bb[++index] = (byte) (x >> 48);
        if (bb[index] != 0 && retLen == -1)
            retLen = 7;
        bb[++index] = (byte) (x >> 40);
        if (bb[index] != 0 && retLen == -1)
            retLen = 6;
        bb[++index] = (byte) (x >> 32);
        if (bb[index] != 0 && retLen == -1)
            retLen = 5;
        bb[++index] = (byte) (x >> 24);
        if (bb[index] != 0 && retLen == -1)
            retLen = 4;
        bb[++index] = (byte) (x >> 16);
        if (bb[index] != 0 && retLen == -1)
            retLen = 3;
        bb[++index] = (byte) (x >> 8);
        if (bb[index] != 0 && retLen == -1)
            retLen = 2;
        bb[++index] = (byte) x;
        if (retLen == -1)
            retLen = 1;
        return Arrays.copyOfRange(bb, bb.length - retLen, bb.length);
    }

    public static void memcpy(byte[] aPtr, int aStart, byte[] bPtr) {
        assert (aPtr.length - aStart + 1 == bPtr.length);
        System.arraycopy(bPtr, 0, aPtr, aStart, bPtr.length);
    }

    /**
     * compare two byte array that the size of a_ptr >=b_ptr
     *
     * @param aPtr
     * @param bPtr
     * @return
     */
    private static int memcmp0(byte[] aPtr, byte[] bPtr) {
        int aLen = aPtr.length, bLen = bPtr.length;
        for (int i = 0; i < aLen - bLen; i++) {
            if (aPtr[i] != 0)
                return 1;
        }
        int aStart = aLen - bLen;
        for (int i = 0; i < bLen; i++) {
            byte aByte = aPtr[aStart + i];
            byte bByte = bPtr[i];
            if (aByte > bByte)
                return 1;
            else if (aByte < bByte)
                return -1;
        }
        return 0;
    }

    /**
     * parser rowpacket, all rowpacket's data is String
     *
     * @param charsetName
     * @param buff
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String getFullString(String charsetName, byte[] buff) throws UnsupportedEncodingException {
        if (buff == null || charsetName == null)
            return null;
        if (Charset.isSupported(charsetName)) {
            return new String(buff, charsetName);
        } else {
            String msg = "unsupported character set :" + charsetName;
            throw new UnsupportedEncodingException(msg);
        }
    }

    public static final long[] LOG_10_INT = new long[]{1, 10, 100, 1000, 10000L, 100000L, 1000000L, 10000000L, 100000000L,
            1000000000L, 10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L,
            1000000000000000L, 10000000000000000L, 100000000000000000L, 1000000000000000000L};

    public static long pow10(int index) {
        return (long) Math.pow(10, index);
    }

    public static final String NULLS = null;

    public static int checkWord(String[] nameArray, char[] cs, int offset, int count) {
        String val = new String(cs, offset, count);
        for (int index = 0; index < nameArray.length; index++) {
            if (val.equalsIgnoreCase(nameArray[index]))
                return index;
        }
        return 0;
    }
}
