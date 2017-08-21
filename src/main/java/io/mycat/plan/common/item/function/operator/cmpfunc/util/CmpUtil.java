package io.mycat.plan.common.item.function.operator.cmpfunc.util;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemResult;
import io.mycat.plan.common.ptr.BoolPtr;
import io.mycat.plan.common.ptr.ItemResultPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimeStatus;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

/**
 * compare用到的一些公共方法
 */
public class CmpUtil {
    /**
     * Parse date provided in a string to a MYSQL_TIME.
     *
     * @return Status flag
     * @param[in] thd Thread handle
     * @param[in] str A string to convert
     * @param[in] warn_type Type of the timestamp for issuing the warning
     * @param[in] warn_name Field name for issuing the warning
     * @param[out] l_time The MYSQL_TIME objects is initialized.
     * <p>
     * Parses a date provided in the string str into a MYSQL_TIME
     * object. If the string contains an incorrect date or doesn't
     * correspond to a date at all then a warning is issued. The
     * warn_type and the warn_name arguments are used as the name
     * and the type of the field when issuing the warning. If any
     * input was discarded (trailing or non-timestamp-y characters),
     * return value will be TRUE.
     * @retval FALSE Success.
     * @retval True Indicates failure.
     */

    public static boolean get_mysql_time_from_str(String str, MySQLTimestampType warn_type,
                                                  final String warn_name, MySQLTime l_time) {
        boolean value;
        MySQLTimeStatus status = new MySQLTimeStatus();
        if (!MyTime.str_to_datetime(str, str.length(), l_time, MyTime.TIME_FUZZY_DATE, status)
                && (l_time.time_type == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME
                || l_time.time_type == MySQLTimestampType.MYSQL_TIMESTAMP_DATE))
            /*
             * Do not return yet, we may still want to throw a
			 * "trailing garbage" warning.
			 */
            value = false;
        else {
            value = true;
            status.warnings = MyTime.MYSQL_TIME_WARN_TRUNCATED; /*
                                                                 * force warning
																 */
        }

        if (status.warnings > 0)
            ;
        // make_truncated_value_warning(thd, Sql_condition::SL_WARNING,
        // ErrConvString(str), warn_type, warn_name);

        return value;
    }

    /**
     * @return converted value. 0 on error and on zero-dates -- check 'failure'
     * @brief Convert date provided in a string to its packed temporal int
     * representation.
     * @param[in] thd thread handle
     * @param[in] str a string to convert
     * @param[in] warn_type type of the timestamp for issuing the warning
     * @param[in] warn_name field name for issuing the warning
     * @param[out] error_arg could not extract a DATE or DATETIME
     * @details Convert date provided in the string str to the int
     * representation. If the string contains wrong date or doesn't
     * contain it at all then a warning is issued. The warn_type and
     * the warn_name arguments are used as the name and the type of the
     * field when issuing the warning.
     */
    public static long get_date_from_str(String str, MySQLTimestampType warn_type, String warn_name,
                                         BoolPtr error_arg) {
        MySQLTime l_time = new MySQLTime();
        error_arg.set(get_mysql_time_from_str(str, warn_type, warn_name, l_time));

        if (error_arg.get())
            return 0;
        return MyTime.TIME_to_longlong_datetime_packed(l_time);
    }

    /**
     * Aggregates result types from the array of items.
     * <p>
     * SYNOPSIS: agg_cmp_type() type [out] the aggregated type items array of
     * items to aggregate the type from nitems number of items in the array
     * <p>
     * DESCRIPTION This function aggregates result types from the array of
     * items. Found type supposed to be used later for comparison of values of
     * these items. Aggregation itself is performed by the item_cmp_type()
     * function.
     *
     * @param items  array of items to aggregate the type from
     * @param nitems number of items in the array
     * @param[out] type the aggregated type
     * @retval 1 type incompatibility has been detected
     * @retval 0 otherwise
     */
    public static int agg_cmp_type(ItemResultPtr type, List<Item> items, int nitems) {
        int i;
        type.set(items.get(0).resultType());
        for (i = 1; i < nitems; i++) {
            type.set(MySQLcom.item_cmp_type(type.get(), items.get(i).resultType()));
            /*
             * When aggregating types of two row expressions we have to check
			 * that they have the same cardinality and that each component of
			 * the first row expression has a compatible row signature with the
			 * signature of the corresponding component of the second row
			 * expression.
			 */
            if (type.get() == ItemResult.ROW_RESULT && MySQLcom.cmpRowType(items.get(0), items.get(i)) != 0)
                return 1; // error found: invalid usage of rows
        }
        return 0;
    }
}
