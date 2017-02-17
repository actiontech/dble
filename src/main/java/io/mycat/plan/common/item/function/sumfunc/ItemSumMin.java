package io.mycat.plan.common.item.function.sumfunc;

import java.io.Serializable;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


public class ItemSumMin extends ItemSumHybrid {

	public ItemSumMin(List<Item> args, boolean distinct, boolean isPushDown, List<Field> fields) {
		super(args, distinct, 1, isPushDown, fields);
	}

	@Override
	public Sumfunctype sumType() {
		return Sumfunctype.MIN_FUNC;
	}

	private static class AggData implements Serializable {

		private static final long serialVersionUID = 5265691791812484350L;
		public byte[] ptr;
		public boolean isNull = false;

		public AggData(byte[] ptr, boolean isNull) {
			this.ptr = ptr;
			this.isNull = isNull;
		}

	}

	@Override
	public Object getTransAggObj() {
		AggData data = new AggData(value.ptr, nullValue);
		return data;
	}

	@Override
	public boolean add(RowDataPacket row, Object transObj) {
		if (transObj != null) {
			AggData data = (AggData) transObj;
			byte[] b1 = data.ptr;
			byte[] b0 = value.ptr;
			if (!data.isNull && (nullValue || value.compare(b0, b1) > 0)) {
				value.setPtr(b1);
				nullValue = false;
			}
		} else {
			byte[] b1 = args.get(0).getRowPacketByte();
			byte[] b0 = value.ptr;
			if (!args.get(0).isNull() && (nullValue || value.compare(b0, b1) > 0)) {
				value.setPtr(b1);
				nullValue = false;
			}
		}
		return false;
	}

	/**
	 * min(id)的pushdown为min(id)
	 */
	@Override
	public boolean pushDownAdd(RowDataPacket row) {
		byte[] b1 = args.get(0).getRowPacketByte();
		byte[] b0 = value.ptr;
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
		if(has_with_distinct()){
			aggregate.setOption(SQLAggregateOption.DISTINCT);
		}
		return aggregate;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		if (!forCalculate) {
			List<Item> newArgs = cloneStructList(args);
			return new ItemSumMin(newArgs,has_with_distinct(), false, null);
		} else {
			return new ItemSumMin(calArgs,has_with_distinct(), isPushDown, fields);
		}
	}

}
