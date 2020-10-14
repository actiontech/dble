/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;

import java.io.Serializable;
import java.util.List;


public class ItemSumMin extends ItemSumHybrid {

    public ItemSumMin(List<Item> args, boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(args, 1, isPushDown, fields, charsetIndex);
    }

    @Override
    public SumFuncType sumType() {
        return SumFuncType.MIN_FUNC;
    }

    @Override
    public Object getTransAggObj() {
        AggData data = new AggData(value.getPtr(), nullValue);
        return data;
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            AggData data = (AggData) transObj;
            byte[] b1 = data.ptr;
            byte[] b0 = value.getPtr();
            if (!data.isNull && (nullValue || value.compare(b0, b1) > 0)) {
                value.setPtr(b1);
                nullValue = false;
            }
        } else {
            byte[] b1 = args.get(0).getRowPacketByte();
            byte[] b0 = value.getPtr();
            if (!args.get(0).isNull() && (nullValue || value.compare(b0, b1) > 0)) {
                value.setPtr(b1);
                nullValue = false;
            }
        }
        return false;
    }

    /**
     * min(id)'s push-downis min(id)
     */
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        byte[] b1 = args.get(0).getRowPacketByte();
        byte[] b0 = value.getPtr();
        if (!args.get(0).isNull() && (nullValue || value.compare(b0, b1) > 0)) {
            value.setPtr(b1);
            nullValue = false;
        }
        return false;
    }

    @Override
    public String funcName() {
        return "MIN";
    }

    @Override
    public SQLExpr toExpression() {
        Item arg0 = args.get(0);
        SQLAggregateExpr aggregate = new SQLAggregateExpr(funcName());
        aggregate.addArgument(arg0.toExpression());
        return aggregate;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumMin(newArgs, false, null, charsetIndex);
        } else {
            return new ItemSumMin(calArgs, isPushDown, fields, charsetIndex);
        }
    }

    private static class AggData implements Serializable {

        private static final long serialVersionUID = 5265691791812484350L;
        private byte[] ptr;
        private boolean isNull = false;

        AggData(byte[] ptr, boolean isNull) {
            this.ptr = ptr;
            this.isNull = isNull;
        }

    }

}
