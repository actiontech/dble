package io.mycat.plan.common.item;

import com.alibaba.druid.sql.ast.SQLExpr;
import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.plan.common.context.NameResolutionContext;
import io.mycat.plan.common.context.ReferContext;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.time.MySQLTime;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemRef extends Item {
    public enum Ref_Type {
        REF, DIRECT_REF, VIEW_REF, OUTER_REF, AGGREGATE_REF
    }

    ;

    private Item ref;
    private String tableAlias = null;
    private String fieldAlias = null;

    public ItemRef(Item ref, String table_alias, String field_alias) {
        this.ref = ref;
        this.tableAlias = table_alias;
        this.fieldAlias = field_alias;
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
            tmpFp.orgName = tmpFp.name;
            try {
                tmpFp.name = fieldAlias.getBytes(CharsetUtil.getJavaCharset(charsetIndex));
            } catch (UnsupportedEncodingException e) {
                LOGGER.warn("parse string exception!", e);
            }
        }
        if (tableAlias != null) {
            tmpFp.orgTable = tmpFp.table;
            try {
                tmpFp.table = tableAlias.getBytes(CharsetUtil.getJavaCharset(charsetIndex));
            } catch (UnsupportedEncodingException e) {
                LOGGER.warn("parse string exception!", e);
            }
        }
    }

    @Override
    public Item fixFields(NameResolutionContext context) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected usage!");
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
