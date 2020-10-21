/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.plan.common.time.DateTimeFormat;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;


public class ItemFuncDateFormat extends ItemStrFunc {
    private boolean isTimeFormat;

    public ItemFuncDateFormat(List<Item> args, boolean isTimeFormat, int charsetIndex) {
        super(args, charsetIndex);
        this.isTimeFormat = isTimeFormat;
    }

    @Override
    public final String funcName() {
        return "date_format";
    }

    @Override
    public String valStr() {
        String format;
        MySQLTime lTime = new MySQLTime();
        int size;
        if (!isTimeFormat) {
            if (getArg0Date(lTime, MyTime.TIME_FUZZY_DATE))
                return null;
        } else {
            if (getArg0Time(lTime))
                return null;
            lTime.setDay(0);
            lTime.setMonth(0);
            lTime.setYear(0);
        }
        if ((format = args.get(1).valStr()) == null || format.length() == 0) {
            nullValue = true;
            return null;
        }
        size = formatLength(format);
        if (size < MyTime.MAX_DATE_STRING_REP_LENGTH)
            size = MyTime.MAX_DATE_STRING_REP_LENGTH;
        DateTimeFormat dateTimeFormat = new DateTimeFormat();
        dateTimeFormat.setFormat(format);
        StringPtr strPtr = new StringPtr("");
        if (!MyTime.makeDateTime(dateTimeFormat, lTime,
                isTimeFormat ? MySQLTimestampType.MYSQL_TIMESTAMP_TIME : MySQLTimestampType.MYSQL_TIMESTAMP_DATE,
                strPtr)) {
            return strPtr.get();
        }
        nullValue = true;
        return null;

    }

    private int formatLength(final String sformat) {
        char[] format = sformat.toCharArray();
        int size = 0;
        int ptr = 0;
        int end = format.length;

        for (; ptr != end; ptr++) {
            if (format[ptr] != '%' || ptr == end - 1)
                size++;
            else {
                switch (format[++ptr]) {
                    case 'M': /* month, textual */
                    case 'W': /* day (of the week), textual */
                        size += 64; /* large for UTF8 locale data */
                        break;
                    case 'D': /* day (of the month), numeric plus english suffix */
                    case 'Y': /* year, numeric, 4 digits */
                    case 'x': /* Year, used with 'v' */
                    case 'X': /*
                             * Year, used with 'v, where week starts with
                             * Monday'
                             */
                        size += 4;
                        break;
                    case 'a': /* locale's abbreviated weekday name (Sun..Sat) */
                    case 'b': /* locale's abbreviated month name (Jan.Dec) */
                        size += 32; /* large for UTF8 locale data */
                        break;
                    case 'j': /* day of year (001..366) */
                        size += 3;
                        break;
                    case 'U': /* week (00..52) */
                    case 'u': /* week (00..52), where week starts with Monday */
                    case 'V': /* week 1..53 used with 'x' */
                    case 'v': /*
                             * week 1..53 used with 'x', where week starts with
                             * Monday
                             */
                    case 'y': /* year, numeric, 2 digits */
                    case 'm': /* month, numeric */
                    case 'd': /* day (of the month), numeric */
                    case 'h': /* hour (01..12) */
                    case 'I': /* --||-- */
                    case 'i': /* minutes, numeric */
                    case 'l': /* hour ( 1..12) */
                    case 'p': /* locale's AM or PM */
                    case 'S': /* second (00..61) */
                    case 's': /* seconds, numeric */
                    case 'c': /* month (0..12) */
                    case 'e': /* day (0..31) */
                        size += 2;
                        break;
                    case 'k': /* hour ( 0..23) */
                    case 'H': /*
                             * hour (00..23; value > 23 OK, padding always
                             * 2-digit)
                             */
                        size += 7; /*
                                 * docs allow > 23, range depends on
                                 * sizeof(unsigned int)
                                 */
                        break;
                    case 'r': /* time, 12-hour (hh:mm:ss [AP]M) */
                        size += 11;
                        break;
                    case 'T': /* time, 24-hour (hh:mm:ss) */
                        size += 8;
                        break;
                    case 'f': /* microseconds */
                        size += 6;
                        break;
                    case 'w': /* day (of the week), numeric */
                    case '%':
                    default:
                        size++;
                        break;
                }
            }
        }
        return size;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncDateFormat(realArgs, isTimeFormat, charsetIndex);
    }
}
