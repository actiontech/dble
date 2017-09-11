/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.string;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item.ItemResult;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class FieldStr extends Field {

    public FieldStr(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    @Override
    public BigInteger valInt() {
        return valReal().toBigInteger();
    }

    @Override
    public BigDecimal valReal() {
        if (ptr == null)
            return BigDecimal.ZERO;
        else {
            String ptrStr = null;
            try {
                ptrStr = MySQLcom.getFullString(charsetName, ptr);
            } catch (UnsupportedEncodingException ue) {
                LOGGER.warn("parse string exception!", ue);
                return BigDecimal.ZERO;
            }
            try {
                return new BigDecimal(ptrStr);
            } catch (Exception e) {
                LOGGER.info("String:" + ptrStr + " to BigDecimal exception!", e);
                return BigDecimal.ZERO;
            }
        }
    }

    @Override
    public BigDecimal valDecimal() {
        if (ptr == null)
            return null;
        else {
            String ptrStr = null;
            try {
                ptrStr = MySQLcom.getFullString(charsetName, ptr);
            } catch (UnsupportedEncodingException ue) {
                LOGGER.warn("parse string exception!", ue);
                return null;
            }
            try {
                return new BigDecimal(ptrStr);
            } catch (Exception e) {
                LOGGER.info("String:" + ptrStr + " to BigDecimal exception!", e);
                return null;
            }
        }
    }

    @Override
    public ItemResult numericContextResultType() {
        return ItemResult.REAL_RESULT;
    }

    public boolean binary() {
        return false;
    }

    @Override
    protected void internalJob() {
    }

    @Override
    public int compareTo(final Field other) {
        if (other == null || !(other instanceof FieldStr))
            return 1;
        FieldStr other2 = (FieldStr) other;
        String ptrStr = this.valStr();
        String ptrStr2 = other2.valStr();
        if (ptrStr == null && ptrStr2 == null)
            return 0;
        else if (ptrStr2 == null)
            return 1;
        else if (ptrStr == null)
            return -1;
        else
            return ptrStr.compareTo(ptrStr2);
    }

    @Override
    public int compare(byte[] v1, byte[] v2) {
        if (v1 == null && v2 == null)
            return 0;
        else if (v1 == null) {
            return -1;
        } else if (v2 == null) {
            return 1;
        }
        try {
            // mysql order by,>,< use UpperCase to compare
            String sval1 = MySQLcom.getFullString(charsetName, v1).toUpperCase();
            String sval2 = MySQLcom.getFullString(charsetName, v2).toUpperCase();
            return sval1.compareTo(sval2);
        } catch (Exception e) {
            LOGGER.info("String to biginteger exception!", e);
            return -1;
        }
    }

}
