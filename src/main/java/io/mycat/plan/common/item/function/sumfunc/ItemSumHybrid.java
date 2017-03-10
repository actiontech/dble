package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.field.FieldUtil;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;


public abstract class ItemSumHybrid extends ItemSum {

	protected Field value;
	protected ItemResult hybrid_type;
	protected FieldTypes hybrid_field_type;
	protected int cmp_sign;
	protected boolean was_values;// Set if we have found at least one row (for
									// max/min only)

	public ItemSumHybrid(List<Item> args, int sign, boolean isPushDown, List<Field> fields) {
		super(args, isPushDown, fields);
		hybrid_field_type = FieldTypes.MYSQL_TYPE_LONGLONG;
		hybrid_type = ItemResult.INT_RESULT;
		cmp_sign = sign;
		was_values = true;
	}

	@Override
	public boolean fixFields() {
		Item item = args.get(0);

		// 'item' can be changed during fix_fields
		if (!item.fixed && item.fixFields())
			return true;
		item = args.get(0);
		decimals = item.decimals;
		value = Field.getFieldItem(funcName(), null, item.fieldType().numberValue(), item.charsetIndex,
				item.maxLength, item.decimals, (item.maybeNull ? 0 : FieldUtil.NOT_NULL_FLAG));

		switch (hybrid_type = item.resultType()) {
		case INT_RESULT:
		case DECIMAL_RESULT:
		case STRING_RESULT:
			maxLength = item.maxLength;
			break;
		case REAL_RESULT:
			maxLength = floatLength(decimals);
			break;
		case ROW_RESULT:
		default:
			assert (false);
		}
		charsetIndex = item.charsetIndex;
		/*
		 * MIN/MAX can return NULL for empty set indepedent of the used column
		 */
		maybeNull = true;
		nullValue = true;
		fixLengthAndDec();
		hybrid_field_type = item.fieldType();

		fixed = true;
		return false;
	}

	@Override
	public void clear() {
		value.setPtr(null);
		nullValue = true;
	}

	@Override
	public BigDecimal valReal() {
		if (nullValue)
			return BigDecimal.ZERO;
		BigDecimal retval = value.valReal();
		if (nullValue = value.isNull())
			retval = BigDecimal.ZERO;
		return retval;
	}

	@Override
	public BigInteger valInt() {
		if (nullValue)
			return BigInteger.ZERO;
		BigInteger retval = value.valInt();
		if (nullValue = value.isNull())
			retval = BigInteger.ZERO;
		return retval;
	}

	@Override
	public long valTimeTemporal() {
		if (nullValue)
			return 0;
		long retval = value.valTimeTemporal();
		if (nullValue = value.isNull())
			retval = 0;
		return retval;
	}

	@Override
	public long valDateTemporal() {
		if (nullValue)
			return 0;
		long retval = value.valDateTemporal();
		if (nullValue = value.isNull())
			retval = 0;
		return retval;
	}

	@Override
	public BigDecimal valDecimal() {
		if (nullValue)
			return null;
		BigDecimal retval = value.valDecimal();
		if (nullValue = value.isNull())
			retval = null;
		return retval;
	}
	
	@Override
	public int getTransSize() {
		return value.fieldLength;
	}
	
	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		if (nullValue)
			return true;
		return (nullValue = value.getDate(ltime, fuzzydate));
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		if (nullValue)
			return true;
		return (nullValue = value.getTime(ltime));
	}

	@Override
	public String valStr() {
		if (nullValue)
			return null;
		String retval = value.valStr();
		if (nullValue = value.isNull())
			retval = null;
		return retval;
	}

	@Override
	public ItemResult resultType() {
		return hybrid_type;
	}

	@Override
	public FieldTypes fieldType() {
		return hybrid_field_type;
	}

	@Override
	public void cleanup() {
		super.cleanup();
		/*
		 * by default it is TRUE to avoid TRUE reporting by
		 * Item_func_not_all/Item_func_nop_all if this item was never called.
		 * 
		 * no_rows_in_result() set it to FALSE if was not results found. If some
		 * results found it will be left unchanged.
		 */
		was_values = true;
	}

	public boolean any_value() {
		return was_values;
	}

	@Override
	public void noRowsInResult() {
		was_values = false;
		clear();
	}

}
