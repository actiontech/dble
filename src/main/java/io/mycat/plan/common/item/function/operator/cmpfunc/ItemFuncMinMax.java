package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

/**
 * min_max函数的父函数，通过cmp_sign来区分是min还是max函数
 * 
 * 
 */
public abstract class ItemFuncMinMax extends ItemFunc {
	ItemResult cmp_type;
	String tmp_value;
	int cmp_sign;
	boolean compare_as_dates;
	Item datetime_item;

	protected FieldTypes cached_field_type;

	/*
	 * Compare item arguments in the DATETIME context.
	 * 
	 * SYNOPSIS cmp_datetimes() value [out] found least/greatest DATE/DATETIME
	 * value
	 * 
	 * DESCRIPTION Compare item arguments as DATETIME values and return the
	 * index of the least/greatest argument in the arguments array. The correct
	 * integer DATE/DATETIME value of the found argument is stored to the value
	 * pointer, if latter is provided.
	 * 
	 * RETURN 0 If one of arguments is NULL or there was a execution error #
	 * index of the least/greatest argument
	 */
	protected long cmp_datetimes(LongPtr value) {
		long min_max = -1;
		int min_max_idx = 0;

		for (int i = 0; i < args.size(); i++) {
			long res = args.get(i).valDateTemporal();

			if ((nullValue = args.get(i).isNull()))
				return 0;
			if (i == 0 || (res < min_max ? cmp_sign : -cmp_sign) > 0) {
				min_max = res;
				min_max_idx = i;
			}
		}
		value.set(min_max);
		return min_max_idx;
	}

	protected long cmp_times(LongPtr value) {
		long min_max = -1;
		int min_max_idx = 0;

		for (int i = 0; i < args.size(); i++) {
			long res = args.get(i).valTimeTemporal();

			if ((nullValue = args.get(i).isNull()))
				return 0;
			if (i == 0 || (res < min_max ? cmp_sign : -cmp_sign) > 0) {
				min_max = res;
				min_max_idx = i;
			}
		}
		value.set(min_max);
		return min_max_idx;
	}

	public ItemFuncMinMax(List<Item> args, int cmp_sign_arg) {
		super(args);
		this.cmp_sign = cmp_sign_arg;
		cmp_type = ItemResult.INT_RESULT;
		compare_as_dates = false;
		datetime_item = null;
	}

	@Override
	public BigDecimal valReal() {
		double value = 0.0;
		if (compare_as_dates) {
			LongPtr result = new LongPtr(0);
			cmp_datetimes(result);
			return new BigDecimal(MyTime.double_from_datetime_packed(datetime_item.fieldType(), result.get()));
		}
		for (int i = 0; i < args.size(); i++) {
			if (i == 0)
				value = args.get(i).valReal().doubleValue();
			else {
				double tmp = args.get(i).valReal().doubleValue();
				if (!args.get(i).isNull() && (tmp < value ? cmp_sign : -cmp_sign) > 0)
					value = tmp;
			}
			if ((nullValue = args.get(i).isNull()))
				break;
		}
		return new BigDecimal(value);
	}

	@Override
	public BigInteger valInt() {
		long value = 0;
		if (compare_as_dates) {
			LongPtr result = new LongPtr(0);
			cmp_datetimes(result);
			return BigInteger.valueOf(MyTime.longlong_from_datetime_packed(datetime_item.fieldType(), result.get()));
		}
		/*
		 * TS-TODO: val_str decides which type to use using cmp_type. val_int,
		 * val_decimal, val_real do not check cmp_type and decide data type
		 * according to the method type. This is probably not good:
		 * 
		 * mysql> select least('11', '2'), least('11', '2')+0,
		 * concat(least(11,2));
		 * +------------------+--------------------+---------------------+ |
		 * least('11', '2') | least('11', '2')+0 | concat(least(11,2)) |
		 * +------------------+--------------------+---------------------+ | 11
		 * | 2 | 2 |
		 * +------------------+--------------------+---------------------+ 1 row
		 * in set (0.00 sec)
		 * 
		 * Should not the second column return 11? I.e. compare as strings and
		 * return '11', then convert to number.
		 */
		for (int i = 0; i < args.size(); i++) {
			if (i == 0)
				value = args.get(i).valInt().longValue();
			else {
				long tmp = args.get(i).valInt().longValue();
				if (!args.get(i).isNull() && (tmp < value ? cmp_sign : -cmp_sign) > 0)
					value = tmp;
			}
			if ((nullValue = args.get(i).isNull()))
				break;
		}
		return BigInteger.valueOf(value);
	}

	@Override
	public String valStr() {
		if (compare_as_dates) {
			if (isTemporal()) {
				/*
				 * In case of temporal data types, we always return string value
				 * according the format of the data type. For example, in case
				 * of LEAST(time_column, datetime_column) the result date type
				 * is DATETIME, so we return a 'YYYY-MM-DD hh:mm:ss' string even
				 * if time_column wins (conversion from TIME to DATETIME happens
				 * in this case).
				 */
				LongPtr result = new LongPtr(0);
				cmp_datetimes(result);
				if (nullValue)
					return null;
				MySQLTime ltime = new MySQLTime();
				MyTime.TIME_from_longlong_packed(ltime, fieldType(), result.get());
				return MyTime.my_time_to_str(ltime, decimals);

			} else {
				/*
				 * In case of VARCHAR result type we just return val_str() value
				 * of the winning item AS IS, without conversion.
				 */
				long min_max_idx = cmp_datetimes(new LongPtr(0));
				if (nullValue)
					return null;
				String str_res = args.get((int) min_max_idx).valStr();
				if (args.get((int) min_max_idx).nullValue) {
					// check if the call to val_str() above returns a NULL value
					nullValue = true;
					return null;
				}
				return str_res;
			}
		}

		if (cmp_type == ItemResult.INT_RESULT) {
			BigInteger nr = valInt();
			if (nullValue)
				return null;
			return nr.toString();
		} else if (cmp_type == ItemResult.DECIMAL_RESULT) {
			BigDecimal bd = valDecimal();
			if (nullValue)
				return null;
			return bd.toString();
		} else if (cmp_type == ItemResult.REAL_RESULT) {
			BigDecimal nr = valReal();
			if (nullValue)
				return null; /* purecov: inspected */
			return nr.toString();
		} else if (cmp_type == ItemResult.STRING_RESULT) {
			String res = null;
			for (int i = 0; i < args.size(); i++) {
				if (i == 0)
					res = args.get(i).valStr();
				else {
					String res2 = args.get(i).valStr();
					if (res2 != null) {
						int cmp = res.compareTo(res2);
						if ((cmp_sign < 0 ? cmp : -cmp) < 0)
							res = res2;
					}
				}
				if ((nullValue = args.get(i).isNull()))
					return null;
			}
			return res;
		} else {// This case should never be chosen
			return null;
		}
	}

	@Override
	public BigDecimal valDecimal() {
		BigDecimal res = null, tmp;

		if (compare_as_dates) {
			LongPtr value = new LongPtr(0);
			cmp_datetimes(value);
			return MyTime.my_decimal_from_datetime_packed(datetime_item.fieldType(), value.get());
		}
		for (int i = 0; i < args.size(); i++) {
			if (i == 0)
				res = args.get(i).valDecimal();
			else {
				tmp = args.get(i).valDecimal(); // Zero if NULL
				if (tmp != null && tmp.compareTo(res) * cmp_sign < 0) {
					res = tmp;
				}
			}
			if ((nullValue = args.get(i).isNull())) {
				res = null;
				break;
			}
		}
		return res;
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		assert (fixed == true);
		if (compare_as_dates) {
			LongPtr result = new LongPtr(0);
			cmp_datetimes(result);
			if (nullValue)
				return true;
			MyTime.TIME_from_longlong_packed(ltime, datetime_item.fieldType(), result.get());
			LongPtr warnings = new LongPtr(0);
			return MyTime.check_date(ltime, ltime.isNonZeroDate(), fuzzydate, warnings);
		}

		FieldTypes i = fieldType();
		if (i == FieldTypes.MYSQL_TYPE_TIME) {
			return getDateFromTime(ltime);
		} else if (i == FieldTypes.MYSQL_TYPE_DATETIME || i == FieldTypes.MYSQL_TYPE_TIMESTAMP || i == FieldTypes.MYSQL_TYPE_DATE) {
			assert (false); // Should have been processed in "compare_as_dates"
			// block.

			return getDateFromNonTemporal(ltime, fuzzydate);
		} else {
			return getDateFromNonTemporal(ltime, fuzzydate);
		}
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		assert (fixed == true);
		if (compare_as_dates) {
			LongPtr result = new LongPtr(0);
			cmp_datetimes(result);
			if (nullValue)
				return true;
			MyTime.TIME_from_longlong_packed(ltime, datetime_item.fieldType(), result.get());
			MyTime.datetime_to_time(ltime);
			return false;
		}

		FieldTypes i = fieldType();
		if (i == FieldTypes.MYSQL_TYPE_TIME) {
			LongPtr result = new LongPtr(0);
			cmp_times(result);
			if (nullValue)
				return true;
			MyTime.TIME_from_longlong_time_packed(ltime, result.get());
			return false;
		} else if (i == FieldTypes.MYSQL_TYPE_DATE || i == FieldTypes.MYSQL_TYPE_TIMESTAMP || i == FieldTypes.MYSQL_TYPE_DATETIME) {
			assert (false); // Should have been processed in "compare_as_dates"
			// block.

			return getTimeFromNonTemporal(ltime);
		} else {
			return getTimeFromNonTemporal(ltime);
		}
	}

	@Override
	public void fixLengthAndDec() {
		int string_arg_count = 0;
		boolean datetime_found = false;
		decimals = 0;
		maxLength = 0;
		cmp_type = args.get(0).temporalWithDateAsNumberResultType();

		for (int i = 0; i < args.size(); i++) {
			maxLength = Math.max(maxLength, args.get(i).maxLength);
			decimals = Math.max(decimals, args.get(i).decimals);
			cmp_type = MySQLcom.item_cmp_type(cmp_type, args.get(i).temporalWithDateAsNumberResultType());
			if (args.get(i).resultType() == ItemResult.STRING_RESULT)
				string_arg_count++;
			if (args.get(i).resultType() != ItemResult.ROW_RESULT && args.get(i).isTemporalWithDate()) {
				datetime_found = true;
				if (datetime_item == null || args.get(i).fieldType() == FieldTypes.MYSQL_TYPE_DATETIME)
					datetime_item = args.get(i);
			}
		}

		if (string_arg_count == args.size()) {
			if (datetime_found) {
				compare_as_dates = true;
				/*
				 * We should not do this: cached_field_type=
				 * datetime_item->field_type(); count_datetime_length(args,
				 * arg_count); because compare_as_dates can be TRUE but result
				 * type can still be VARCHAR.
				 */
			}
		}
		cached_field_type = MySQLcom.agg_field_type(args, 0, args.size());
	}

	@Override
	public ItemResult resultType() {
		return compare_as_dates ? ItemResult.STRING_RESULT : cmp_type;
	}

	@Override
	public FieldTypes fieldType() {
		return cached_field_type;
	}

	public ItemResult castToIntType() {
		/*
		 * make CAST(LEAST_OR_GREATEST(datetime_expr, varchar_expr)) return a
		 * number in format "YYYMMDDhhmmss".
		 */
		return compare_as_dates ? ItemResult.INT_RESULT : resultType();
	}
}
