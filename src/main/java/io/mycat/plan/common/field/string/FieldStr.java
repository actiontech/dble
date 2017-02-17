package io.mycat.plan.common.field.string;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item.ItemResult;

public abstract class FieldStr extends Field {

	public FieldStr(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
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
			String ptr_str = null;
			try {
				ptr_str = MySQLcom.getFullString(charsetName, ptr);
			} catch (UnsupportedEncodingException ue) {
				logger.warn("parse string exception!", ue);
				return BigDecimal.ZERO;
			}
			try {
				return new BigDecimal(ptr_str);
			} catch (Exception e) {
				logger.info("String:" + ptr_str + " to BigDecimal exception!", e);
				return BigDecimal.ZERO;
			}
		}
	}

	@Override
	public BigDecimal valDecimal() {
		if (ptr == null)
			return null;
		else {
			String ptr_str = null;
			try {
				ptr_str = MySQLcom.getFullString(charsetName, ptr);
			} catch (UnsupportedEncodingException ue) {
				logger.warn("parse string exception!", ue);
				return null;
			}
			try {
				return new BigDecimal(ptr_str);
			} catch (Exception e) {
				logger.info("String:" + ptr_str + " to BigDecimal exception!", e);
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
		String ptr_str = this.valStr();
		String ptr_str2 = other2.valStr();
		if (ptr_str == null && ptr_str2 == null)
			return 0;
		else if (ptr_str2 == null)
			return 1;
		else if (ptr_str == null)
			return -1;
		else
			return ptr_str.compareTo(ptr_str2);
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
			// mysql order by,>,<字符串使用的是排序值，正常是用大写作为比较
			String sval1 = MySQLcom.getFullString(charsetName, v1).toUpperCase();
			String sval2 = MySQLcom.getFullString(charsetName, v2).toUpperCase();
			return sval1.compareTo(sval2);
		} catch (Exception e) {
			logger.info("String to biginteger exception!", e);
			return -1;
		}
	}

}
