/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.common.ptr.ItemResultPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimeStatus;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;

/**
 * CmpUtil
 */
public final class CmpUtil {
    private CmpUtil() {
    }

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

    public static boolean getMysqlTimeFromStr(String str, MySQLTimestampType warnType,
                                              final String warnName, MySQLTime lTime) {
        boolean value;
        MySQLTimeStatus status = new MySQLTimeStatus();
        if (!MyTime.strToDatetime(str, str.length(), lTime, MyTime.TIME_FUZZY_DATE, status) &&
                (lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME ||
                        lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE))
            /*
             * Do not return yet, we may still want to throw a
             * "trailing garbage" warning.
             */
            value = false;
        else {
            value = true;
            status.setWarnings(MyTime.MYSQL_TIME_WARN_TRUNCATED); /*
                                                                 * force warning
                                                                 */
        }

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
    public static long getDateFromStr(String str, MySQLTimestampType warnType, String warnName,
                                      BoolPtr errorArg) {
        MySQLTime lTime = new MySQLTime();
        errorArg.set(getMysqlTimeFromStr(str, warnType, warnName, lTime));

        if (errorArg.get())
            return 0;
        return MyTime.timeToLonglongDatetimePacked(lTime);
    }

    /**
     * Aggregates result types from the array of items.
     * <p>
     * SYNOPSIS: aggCmpType() type [out] the aggregated type items array of
     * items to aggregate the type from nitems number of items in the array
     * <p>
     * DESCRIPTION This function aggregates result types from the array of
     * items. Found type supposed to be used later for comparison of values of
     * these items. Aggregation itself is performed by the itemCmpType()
     * function.
     *
     * @param items  array of items to aggregate the type from
     * @param nitems number of items in the array
     * @param[out] type the aggregated type
     * @retval 1 type incompatibility has been detected
     * @retval 0 otherwise
     */
    public static int aggCmpType(ItemResultPtr type, List<Item> items, int nitems) {
        int i;
        type.set(items.get(0).resultType());
        for (i = 1; i < nitems; i++) {
            type.set(MySQLcom.itemCmpType(type.get(), items.get(i).resultType()));
            /*
             * When aggregating types of two row expressions we have to check
             * that they have the same cardinality and that each component of
             * the first row expression has a compatible row signature with the
             * signature of the corresponding component of the second row
             * expression.
             */
            if (type.get() == Item.ItemResult.ROW_RESULT && MySQLcom.cmpRowType(items.get(0), items.get(i)) != 0)
                return 1; // error found: invalid usage of rows
        }
        return 0;
    }
}
