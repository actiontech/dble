/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field;

import com.actiontech.dble.plan.common.field.string.FieldStr;
import com.actiontech.dble.plan.common.item.FieldTypes;

import java.math.BigDecimal;
import java.math.BigInteger;

public class FieldNull extends FieldStr {
    private static FieldNull instance = new FieldNull("NULL", "", 63, 0, 0, 0);

    public static FieldNull getInstance() {
        return instance;
    }

    FieldNull(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
        this.ptr = null;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_NULL;
    }

    @Override
    public boolean equals(final Field other, boolean binary) {
        return this == other;
    }

    @Override
    public BigDecimal valReal() {
        return BigDecimal.ZERO;
    }

    @Override
    public BigInteger valInt() {
        return BigInteger.ZERO;
    }

    @Override
    public String valStr() {
        return null;
    }

    @Override
    public BigDecimal valDecimal() {
        return null;
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    protected void internalJob() {
    }

    @Override
    public int compareTo(Field other) {
        return -1;
    }

    @Override
    public int compare(byte[] v1, byte[] v2) {
        return -1;
    }

}
