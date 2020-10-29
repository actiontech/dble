/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemField extends ItemIdent {

    private Field field;

    /* if index!=-1, means the index of Item_field,need setField before val */
    private int index = -1;

    public ItemField(String dbName, String tableName, String fieldName, int charsetIndex) {
        super(dbName, tableName, fieldName);
        this.charsetIndex = charsetIndex;
    }

    public ItemField(String dbName, String tableName, String fieldName) {
        super(dbName, tableName, fieldName);
    }

    public ItemField(Field field) {
        super(null, field.getTable(), field.getName());
        setField(field);
    }

    /**
     * save index
     *
     * @param index
     */
    public ItemField(int index) {
        super(null, "", "");
        this.index = index;
    }

    public void setField(List<Field> fields) {
        assert (fields != null);
        setField(fields.get(index));
    }

    protected void setField(Field field) {
        this.field = field;
        maybeNull = field.maybeNull();
        decimals = field.getDecimals();
        tableName = field.getTable();
        itemName = field.getName();
        dbName = field.getDbName();
        maxLength = field.getFieldLength();
        charsetIndex = field.getCharsetIndex();
        fixed = true;
    }

    @Override
    public ItemType type() {
        return ItemType.FIELD_ITEM;
    }

    @Override
    public ItemResult resultType() {
        return field == null ? null : field.resultType();
    }

    @Override
    public ItemResult numericContextResultType() {
        return field.numericContextResultType();
    }

    @Override
    public FieldTypes fieldType() {
        return field.fieldType();
    }

    @Override
    public byte[] getRowPacketByte() {
        return field.getPtr();
    }

    public ItemResult cmpType() {
        return field.cmpType();
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int hashCode = dbName == null ? 0 : dbName.hashCode();
        hashCode = hashCode * prime + (tableName == null ? 0 : tableName.hashCode());
        hashCode = hashCode * prime + (itemName == null ? 0 : itemName.hashCode());
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ItemField))
            return false;
        ItemField other = (ItemField) obj;
        return StringUtils.equals(getTableName(), other.getTableName()) &&
                StringUtils.equalsIgnoreCase(getItemName(), other.getItemName());
    }

    @Override
    public BigDecimal valReal() {
        if (nullValue = field.isNull())
            return BigDecimal.ZERO;
        return field.valReal();
    }

    @Override
    public BigInteger valInt() {
        if (nullValue = field.isNull())
            return BigInteger.ZERO;
        return field.valInt();
    }

    @Override
    public long valTimeTemporal() {
        if ((nullValue = field.isNull()))
            return 0;
        return field.valTimeTemporal();
    }

    @Override
    public long valDateTemporal() {
        if ((nullValue = field.isNull()))
            return 0;
        return field.valDateTemporal();
    }

    @Override
    public BigDecimal valDecimal() {
        if (nullValue = field.isNull())
            return null;
        return field.valDecimal();
    }

    @Override
    public String valStr() {
        if (nullValue = field.isNull())
            return null;
        return field.valStr();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        if ((nullValue = field.isNull()) || field.getDate(ltime, fuzzydate)) {
            ltime.setZeroTime(ltime.getTimeType());
            return true;
        }
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        if ((nullValue = field.isNull()) || field.getTime(ltime)) {
            ltime.setZeroTime(ltime.getTimeType());
            return true;
        }
        return false;
    }

    @Override
    public boolean isNull() {
        return field.isNull();
    }

    @Override
    public void makeField(FieldPacket fp) {
        field.makeField(fp);
        try {
            if (itemName != null) {
                fp.setName(itemName.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            }
            if ((tableName != null)) {
                fp.setTable(tableName.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            }
            if (dbName != null) {
                fp.setDb(dbName.getBytes(CharsetUtil.getJavaCharset(charsetIndex)));
            }
        } catch (UnsupportedEncodingException e) {
            LOGGER.info("parse string exception!", e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public Item fixFields(NameResolutionContext context) {
        if (this.isWild())
            return this;
        String tmpFieldName = getItemName();
        PlanNode planNode = context.getPlanNode();
        if (context.getPlanNode().type() == PlanNodeType.MERGE) {
            return getMergeNodeColumn(tmpFieldName, planNode);
        }
        Item column = null;
        if (context.isFindInSelect()) {
            // try to find in selectlist
            if (StringUtils.isEmpty(getDbName()) || StringUtils.isEmpty(getTableName())) {
                for (NamedField namedField : planNode.getOuterFields().keySet()) {
                    if (StringUtils.equalsIgnoreCase(tmpFieldName, namedField.getName()) &&
                            (StringUtils.isEmpty(getTableName()) || (StringUtils.isEmpty(getDbName()) && StringUtils.equals(getTableName(), namedField.getTable())))) {
                        if (column == null) {
                            column = planNode.getOuterFields().get(namedField);
                        } else
                            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "42S22", "duplicate column:" + this);
                    }
                }
            } else {
                column = planNode.getOuterFields().get(new NamedField(getDbName(), getTableName(), tmpFieldName, null));
            }
        }
        if (column != null && context.isSelectFirst()) {
            return column;
        }
        return findItemFormInnerField(tmpFieldName, planNode, column);
    }

    private Item findItemFormInnerField(String tmpFieldName, PlanNode planNode, Item column) {
        // find from inner fields
        Item columnFromMeta = null;
        if (StringUtils.isEmpty(getDbName()) || StringUtils.isEmpty(getTableName())) {
            String tmpDbName = null;
            String tmpTableName = null;
            for (NamedField namedField : planNode.getInnerFields().keySet()) {
                if (StringUtils.equalsIgnoreCase(tmpFieldName, namedField.getName()) &&
                        (StringUtils.isEmpty(getTableName()) || (StringUtils.isEmpty(getDbName()) && StringUtils.equals(getTableName(), namedField.getTable())))) {
                    if (columnFromMeta == null) {
                        tmpDbName = namedField.getSchema();
                        tmpTableName = namedField.getTable();
                        getReferTables().clear();
                        NamedField coutField = planNode.getInnerFields().get(new NamedField(namedField.getSchema(), namedField.getTable(), tmpFieldName, null));
                        this.getReferTables().add(coutField.planNode);
                        columnFromMeta = this;
                    } else {
                        if (planNode.type() == PlanNodeType.JOIN) {
                            JoinNode jn = (JoinNode) planNode;
                            if (jn.getUsingFields() != null && jn.getUsingFields().contains(columnFromMeta.getItemName().toLowerCase())) {
                                continue;
                            }
                            throw new MySQLOutPutException(ErrorCode.ER_NON_UNIQ_ERROR, "23000", "Column '" + tmpFieldName + "' in field list is ambiguous");
                        }
                        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "42S22", "duplicate column:" + this);
                    }
                }
            }
            this.dbName = tmpDbName;
            this.tableName = tmpTableName;
        } else {
            NamedField tmpField = new NamedField(getDbName(), getTableName(), tmpFieldName, null);
            if (planNode.getInnerFields().containsKey(tmpField)) {
                NamedField coutField = planNode.getInnerFields().get(tmpField);
                getReferTables().clear();
                getReferTables().add(coutField.planNode);
                this.dbName = tmpField.getSchema();
                this.tableName = tmpField.getTable();
                columnFromMeta = this;
            }
        }
        if (columnFromMeta != null) {
            return columnFromMeta;
        } else if (column == null)
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "42S22", "column " + this + " not found");
        else {
            return column;
        }

    }

    private Item getMergeNodeColumn(String tmpFieldName, PlanNode planNode) {
        String tmpFieldTable = null;
        // select union only found in outerfields
        Item column;
        if (StringUtils.isEmpty(getTableName())) {
            PlanNode firstNode = planNode.getChild();
            boolean found = false;
            for (NamedField coutField : firstNode.getOuterFields().keySet()) {
                if (tmpFieldName.equalsIgnoreCase(coutField.getName())) {
                    if (!found) {
                        tmpFieldTable = coutField.getTable();
                        found = true;
                    } else {
                        throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "(42S22",
                                "Unknown column '" + tmpFieldName + "' in 'order clause'");
                    }
                }
            }
            column = planNode.getOuterFields().get(new NamedField(null, tmpFieldTable, tmpFieldName, null));
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_TABLENAME_NOT_ALLOWED_HERE, "42000",
                    "Table '" + getTableName() + "' from one of the SELECTs cannot be used in global ORDER clause");
        }
        return column;
    }

    @Override
    public void fixRefer(ReferContext context) {
        if (isWild())
            return;
        PlanNode node = context.getPlanNode();
        PlanNode tn = getReferTables().iterator().next();
        node.addSelToReferedMap(tn, this);
    }

    @Override
    public SQLExpr toExpression() {
        SQLIdentifierExpr parent = StringUtil.isEmpty(tableName) ? null : new SQLIdentifierExpr(tableName);
        if (parent != null) {
            return new SQLPropertyExpr(parent, itemName);
        } else return new SQLIdentifierExpr(itemName);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemField(dbName, tableName, itemName, charsetIndex);
    }

    public Field getField() {
        return field;
    }
}
