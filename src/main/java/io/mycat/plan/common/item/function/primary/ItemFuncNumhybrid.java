package io.mycat.plan.common.item.function.primary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;


/**
 * 类型不定的函数，参数是混合类型可能
 * 
 * 
 */
public abstract class ItemFuncNumhybrid extends ItemFunc {
	protected ItemResult hybrid_type;

	public ItemFuncNumhybrid(List<Item> args) {
		super(args);
		hybrid_type = ItemResult.REAL_RESULT;
	}

	@Override
	public ItemResult resultType() {
		return hybrid_type;
	}

	@Override
	public void fixLengthAndDec() {
		fixNumLengthAndDec();
		findNumType();
	}

	public void fixNumLengthAndDec() {

	}

	/* To be called from fix_length_and_dec */
	public abstract void findNumType();

	@Override
	public BigDecimal valReal() {
		switch (hybrid_type) {
		case DECIMAL_RESULT: {
			BigDecimal val = decimalOp();
			if (val == null)
				return BigDecimal.ZERO; // null is set
			return val;
		}
		case INT_RESULT: {
			BigInteger result = intOp();
			return new BigDecimal(result);
		}
		case REAL_RESULT:
			return realOp();
		case STRING_RESULT: {
			switch (fieldType()) {
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIMESTAMP:
				return valRealFromDecimal();
			default:
				break;
			}
			String res = strOp();
			if (res == null)
				return BigDecimal.ZERO;
			else {
				try {
					return new BigDecimal(res);
				} catch (Exception e) {
					logger.error(res + " to BigDecimal error!", e);
				}
			}
		}
		default:
		}
		return BigDecimal.ZERO;
	}

	@Override
	public BigInteger valInt() {
		switch (hybrid_type) {
		case DECIMAL_RESULT: {
			BigDecimal val = decimalOp();
			if (val == null)
				return BigInteger.ZERO;
			return val.toBigInteger();
		}
		case INT_RESULT:
			return intOp();
		case REAL_RESULT:
			return realOp().toBigInteger();
		case STRING_RESULT: {
			switch (fieldType()) {
			case MYSQL_TYPE_DATE:
				return new BigDecimal(valIntFromDate()).toBigInteger();
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIMESTAMP:
				return new BigDecimal(valIntFromDatetime()).toBigInteger();
			case MYSQL_TYPE_TIME:
				return new BigDecimal(valIntFromTime()).toBigInteger();
			default:
				break;
			}
			String res = strOp();
			if (res == null)
				return BigInteger.ZERO;
			try {
				return new BigInteger(res);
			} catch (Exception e) {
				logger.error(res + " to BigInteger error!", e);
			}
		}
		default:
		}
		return BigInteger.ZERO;
	}

	@Override
	public BigDecimal valDecimal() {
		BigDecimal val = null;
		switch (hybrid_type) {
		case DECIMAL_RESULT:
			val = decimalOp();
			break;
		case INT_RESULT: {
			BigInteger result = intOp();
			val = new BigDecimal(result);
			break;
		}
		case REAL_RESULT: {
			BigDecimal result = realOp();
			val = result;
			break;
		}
		case STRING_RESULT: {
			switch (fieldType()) {
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIMESTAMP:
				return valDecimalFromDate();
			case MYSQL_TYPE_TIME:
				return valDecimalFromTime();
			default:
				break;
			}
			String res = strOp();
			if (res == null)
				return null;
			try {
				val = new BigDecimal(res);
			} catch (Exception e) {
				val = null;
			}
			break;
		}
		case ROW_RESULT:
		default:
		}
		return val;
	}

	@Override
	public String valStr() {
		String str = null;
		switch (hybrid_type) {
		case DECIMAL_RESULT: {
			BigDecimal val = decimalOp();
			if (val == null)
				return null; // null is set
			str = val.toString();
			break;
		}
		case INT_RESULT: {
			BigInteger nr = intOp();
			if (nullValue)
				return null; /* purecov: inspected */
			str = nr.toString();
			break;
		}
		case REAL_RESULT: {
			BigDecimal nr = realOp();
			if (nullValue)
				return null; /* purecov: inspected */
			str = nr.toString();
			break;
		}
		case STRING_RESULT:
			switch (fieldType()) {
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIMESTAMP:
				return valStringFromDatetime();
			case MYSQL_TYPE_DATE:
				return valStringFromDate();
			case MYSQL_TYPE_TIME:
				return valStringFromTime();
			default:
				break;
			}
			return strOp();
		default:
		}
		return str;
	}

	@Override
	public boolean getDate(MySQLTime ltime, long flags) {
		assert (fixed == true);
		switch (fieldType()) {
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIMESTAMP:
			return dateOp(ltime, flags);
		case MYSQL_TYPE_TIME:
			return getDateFromTime(ltime);
		default:
			return getDateFromNonTemporal(ltime, flags);
		}
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		assert (fixed == true);
		switch (fieldType()) {
		case MYSQL_TYPE_TIME:
			return timeOp(ltime);
		case MYSQL_TYPE_DATE:
			return getTimeFromDate(ltime);
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_TIMESTAMP:
			return getTimeFromDatetime(ltime);
		default:
			return getTimeFromNonTemporal(ltime);
		}
	}

	/**
	 * @brief Performs the operation that this functions implements when the
	 *        result type is INT.
	 * @return The result of the operation.
	 */
	public abstract BigInteger intOp();

	/**
	 * @brief Performs the operation that this functions implements when the
	 *        result type is REAL.
	 * @return The result of the operation.
	 */
	public abstract BigDecimal realOp();

	/**
	 * @brief Performs the operation that this functions implements when the
	 *        result type is DECIMAL.
	 * @param A
	 *            pointer where the DECIMAL value will be allocated.
	 * @return - 0 If the result is NULL - The same pointer it was given, with
	 *         the area initialized to the result of the operation.
	 */
	public abstract BigDecimal decimalOp();

	/**
	 * @brief Performs the operation that this functions implements when the
	 *        result type is a string type.
	 * @return The result of the operation.
	 */
	public abstract String strOp();

	/**
	 * @brief Performs the operation that this functions implements when the
	 *        result type is MYSQL_TYPE_DATE or MYSQL_TYPE_DATETIME.
	 * @return The result of the operation.
	 */
	public abstract boolean dateOp(MySQLTime ltime, long flags);

	public abstract boolean timeOp(MySQLTime ltime);

}
