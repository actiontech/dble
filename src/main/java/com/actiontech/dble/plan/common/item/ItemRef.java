/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemRef extends Item {
    public enum RefType {
        REF, DIRECT_REF, VIEW_REF, OUTER_REF, AGGREGATE_REF
    }

    private Item ref;
    private String schema = null;
    private String table = null;
    private String tableAlias = null;
    private String fieldAlias = null;

    public ItemRef(Item ref, String schema, String table, String tableAlias, String fieldAlias) {
        this.ref = ref;
        this.schema = schema;
        this.table = table;
        this.tableAlias = tableAlias;
        this.fieldAlias = fieldAlias;
        this.charsetIndex = ref.charsetIndex;
    }

    @Override
    public ItemType type() {
        return ItemType.REF_ITEM;
    }

    @Override
    public boolean fixFields() {
        if (ref == null) {
            // TODO
            throw new RuntimeException("unexpected!");
        }
        return ref.fixFields();
    }

    @Override
    public Item fixFields(NameResolutionContext context) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected usage!");
    }

    @Override
    public BigDecimal valReal() {
        return ref.valReal();
    }

    @Override
    public BigInteger valInt() {
        return ref.valInt();
    }

    @Override
    public long valTimeTemporal() {
        return ref.valTimeTemporal();
    }

    @Override
    public long valDateTemporal() {
        return ref.valDateTemporal();
    }

    @Override
    public BigDecimal valDecimal() {
        return ref.valDecimal();
    }

    @Override
    public boolean valBool() {
        return ref.valBool();
    }

    @Override
    public String valStr() {
        return ref.valStr();
    }

    @Override
    public boolean isNull() {
        return ref.isNull();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return ref.getDate(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return ref.getTime(ltime);
    }

    @Override
    public ItemResult resultType() {
        return ref.resultType();
    }

    @Override
    public FieldTypes fieldType() {
        return ref.fieldType();
    }

    @Override
    public byte[] getRowPacketByte() {
        return ref.getRowPacketByte();
    }

    @Override
    public void makeField(FieldPacket tmpFp) {
        ref.makeField(tmpFp);
        if (fieldAlias != null) {
            tmpFp.setOrgName(tmpFp.getName());
            try {
                tmpFp.setName(fieldAlias.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            } catch (UnsupportedEncodingException e) {
                LOGGER.info("parse string exception!", e);
            }
        }

        if ((schema != null)) {
            try {
                tmpFp.setDb(schema.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            } catch (UnsupportedEncodingException e) {
                LOGGER.info("parse string exception!", e);
            }
        }
        if (table != null) {
            try {
                tmpFp.setOrgTable(table.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
                tmpFp.setTable(table.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            } catch (UnsupportedEncodingException e) {
                LOGGER.info("parse string exception!", e);
            }
        }
        if (tableAlias != null) {
            try {
                tmpFp.setTable(tableAlias.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            } catch (UnsupportedEncodingException e) {
                LOGGER.info("parse string exception!", e);
            }
        }
    }

    @Override
    public void fixRefer(ReferContext context) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected usage!");

    }

    @Override
    public SQLExpr toExpression() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected usage!");
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected usage!");
    }

}
