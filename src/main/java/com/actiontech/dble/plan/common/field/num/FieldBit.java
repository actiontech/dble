/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.num;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;

import java.math.BigDecimal;
import java.math.BigInteger;

public class FieldBit extends Field {
    private BigInteger intValue = null;

    public FieldBit(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_BIT;
    }

    @Override
    public Item.ItemResult resultType() {
        return Item.ItemResult.INT_RESULT;
    }

    @Override
    public BigDecimal valReal() {
        internalJob();
        return isNull() ? BigDecimal.ZERO : new BigDecimal(intValue);
    }

    @Override
    public BigInteger valInt() {
        internalJob();
        return isNull() ? BigInteger.ZERO : intValue;
    }

    @Override
    public BigDecimal valDecimal() {
        internalJob();
        return isNull() ? null : new BigDecimal(intValue);
    }

    @Override
    protected void internalJob() {
        // eg an bit(16) data type,the value is 8737(=34*256+33),we will receive byte[34,33]
        if (ptr != null) {
            long lv = getBitInt(ptr);
            intValue = BigInteger.valueOf(lv);
        }
    }

    @Override
    public int compareTo(Field other) {
        if (other == null || !(other instanceof FieldBit)) {
            return 1;
        }
        FieldBit bOther = (FieldBit) other;
        BigInteger intValue1 = this.valInt();
        BigInteger intValue2 = bOther.valInt();
        if (intValue1 == null && intValue2 == null)
            return 0;
        else if (intValue2 == null)
            return 1;
        else if (intValue1 == null)
            return -1;
        else
            return intValue1.compareTo(intValue2);
    }

    @Override
    public int compare(byte[] v1, byte[] v2) {
        if (v1 == null && v2 == null)
            return 0;
        else if (v1 == null) {
            return -1;
        } else if (v2 == null) {
            return 1;
        } else
            try {
                Long b1 = getBitInt(v1);
                Long b2 = getBitInt(v2);
                return b1.compareTo(b2);
            } catch (Exception e) {
                LOGGER.info("String to biginteger exception!", e);
                return -1;
            }
    }

    /**
     * Big endian
     *
     * @param b
     * @return
     */
    private long getBitInt(byte[] b) {
        if (b == null || b.length == 0)
            return 0;
        int ret = 0;
        int leftShift = 0;
        for (int i = ptr.length - 1; i >= 0; i--) {
            long lb = ptr[i] << leftShift;
            ret += lb;
            leftShift += 8;
        }
        return ret;
    }
}
