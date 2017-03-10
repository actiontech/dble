package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigInteger;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.ItemResultField;
import io.mycat.plan.common.time.MySQLTime;


public abstract class ItemSumNumField extends ItemResultField {
	protected ItemResult hybrid_type;

	@Override
	public BigInteger valInt() {
		return valReal().toBigInteger();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		return getDateFromNumeric(ltime, fuzzydate); /* Decimal or real */
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromNumeric(ltime); /* Decimal or real */
	}

	@Override
	public final FieldTypes fieldType() {
		return hybrid_type == ItemResult.DECIMAL_RESULT ? FieldTypes.MYSQL_TYPE_NEWDECIMAL
				: FieldTypes.MYSQL_TYPE_DOUBLE;
	}

	@Override
	public final ItemResult resultType() {
		return hybrid_type;
	}

	@Override
	public boolean isNull() {
		updateNullValue();
		return nullValue;
	}

}
