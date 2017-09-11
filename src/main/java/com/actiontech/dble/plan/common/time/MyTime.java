/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.time;

import com.actiontech.dble.plan.common.Ctype;
import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.locale.MyLocale;
import com.actiontech.dble.plan.common.locale.MyLocales;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalUnit;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class MyTime {
    private MyTime() {
    }

    private static final DateTimeFormat TIME_AMPM_FORMAT = new DateTimeFormat("%I:%i:%S %p");
    private static final DateTimeFormat TIME_24_HRS_FORMAT = new DateTimeFormat("%H:%i:%S");

    /* Flags to strToDatetime and numberToDatetime */
    public static final int TIME_FUZZY_DATE = 1;
    public static final int TIME_DATETIME_ONLY = 2;
    public static final int TIME_NO_NSEC_ROUNDING = 4;
    public static final int TIME_NO_DATE_FRAC_WARN = 8;

    public static final int DATETIME_MAX_DECIMALS = 6;

    public static final int MAX_DATE_PARTS = 8;
    public static final int MAX_DATE_WIDTH = 10;
    public static final int MAX_TIME_WIDTH = 10;
    public static final int MAX_DATETIME_WIDTH = 19;
    /*
     * -DDDDDD
     * HH:MM:SS.######
     */
    public static final int MAX_TIME_FULL_WIDTH = 23;
    /*
     * YYYY-MM-DD
     * HH:MM:SS.######
     * AM
     */
    public static final int MAX_DATETIME_FULL_WIDTH = 29;
    public static final int MAX_DATE_STRING_REP_LENGTH = 30;

    public static final int TIME_MAX_HOUR = 838;
    public static final int TIME_MAX_MINUTE = 59;
    public static final int TIME_MAX_SECOND = 59;

    public static final int MAX_DAY_NUMBER = 3652424;

    public static final int SECONDS_IN_24H = 86400;

    public static final int TIME_MAX_VALUE = (TIME_MAX_HOUR * 10000 + TIME_MAX_MINUTE * 100 + TIME_MAX_SECOND);

    public static final long TIME_MAX_VALUE_SECONDS = (TIME_MAX_HOUR * 3600L + TIME_MAX_MINUTE * 60L + TIME_MAX_SECOND);

    /* Must be same as MODE_NO_ZERO_IN_DATE */
    public static final long TIME_NO_ZERO_IN_DATE = (65536L * 2 * 2 * 2 * 2 * 2 * 2 * 2);
    /* Must be same as MODE_NO_ZERO_DATE */
    public static final long TIME_NO_ZERO_DATE = (TIME_NO_ZERO_IN_DATE * 2);
    public static final long TIME_INVALID_DATES = (TIME_NO_ZERO_DATE * 2);

    /* Conversion warnings */
    public static final int MYSQL_TIME_WARN_TRUNCATED = 1;
    public static final int MYSQL_TIME_WARN_OUT_OF_RANGE = 2;
    public static final int MYSQL_TIME_WARN_INVALID_TIMESTAMP = 4;
    public static final int MYSQL_TIME_WARN_ZERO_DATE = 8;
    public static final int MYSQL_TIME_NOTE_TRUNCATED = 16;
    public static final int MYSQL_TIME_WARN_ZERO_IN_DATE = 32;

    public static final int YY_PART_YEAR = 70;

    public static final long LONG_MAX = MySQLcom.getUnsignedLong(Integer.MAX_VALUE).longValue();

    static final char TIME_SEPARATOR = ':';

    /* Position for YYYY-DD-MM HH-MM-DD.FFFFFF AM in default format */

    private static final int[] INTERNAL_FORMAT_POSITIONS = {0, 1, 2, 3, 4, 5, 6, 255};

    public static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

    public static final String[] MONTH_NAMES = {"January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "November", "December"};

    public static final String[] AB_MONTH_NAMES = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
            "Oct", "Nov", "Dec"};

    public static final String[] DAY_NAMES = {"Monday", "Tuesday", "Thursday", "Friday", "Saturday", "Sunday"};

    public static final String[] AB_DAY_NAMES = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

    /* Flags for calc_week() function. */
    public static final int WEEK_MONDAY_FIRST = 1;
    public static final int WEEK_YEAR = 2;
    public static final int WEEK_FIRST_WEEKDAY = 4;

    /*-------------------------My_time.h  start------------------------------
     *-------------------------change time type to other type--------------- */

    /*
     * Handle 2 digit year conversions
     *
     * SYNOPSIS year2000Handling() year 2 digit year
     *
     * RETURN Year between 1970-2069
     */

    public static long year2000Handling(long year) {
        if ((year = year + 1900) < 1900 + YY_PART_YEAR)
            year += 100;
        return year;
    }

    /**
     * @return 0 OK 1 error
     * @brief Check datetime value for validity according to flags.
     * @param[in] ltime Date to check.
     * @param[in] not_zero_date ltime is not the zero date
     * @param[in] flags flags to check (see strToDatetime() flags in
     * my_time.h)
     * @param[out] was_cut set to 2 if value was invalid according to flags.
     * (Feb 29 in non-leap etc.) This remains unchanged if value is
     * not invalid.
     * @details Here we assume that year and month is ok! If month is 0 we allow
     * any date. (This only happens if we allow zero date parts in
     * strToDatetime()) Disallow dates with zero year and non-zero
     * month and/or day.
     */

    public static boolean checkDate(final MySQLTime ltime, boolean notZeroDate, long flags, LongPtr wasCut) {
        if (notZeroDate) {
            if (((flags & TIME_NO_ZERO_IN_DATE) != 0 || (flags & TIME_FUZZY_DATE) == 0) &&
                    (ltime.getMonth() == 0 || ltime.getDay() == 0)) {
                wasCut.set(MYSQL_TIME_WARN_ZERO_IN_DATE);
                return true;
            } else if (((flags & TIME_INVALID_DATES) == 0 && ltime.getMonth() != 0 &&
                    ltime.getDay() > DAYS_IN_MONTH[(int) ltime.getMonth() - 1] &&
                    (ltime.getMonth() != 2 || calcDaysInYear(ltime.getYear()) != 366 || ltime.getDay() != 29))) {
                wasCut.set(MYSQL_TIME_WARN_OUT_OF_RANGE);
                return true;
            }
        } else if ((flags & TIME_NO_ZERO_DATE) != 0) {
            wasCut.set(MYSQL_TIME_WARN_ZERO_DATE);
            return true;
        }
        return false;
    }

    /*
     * Convert a timestamp string to a MYSQL_TIME value.
     *
     * SYNOPSIS strToDatetime() str String to parse length Length of string
     * l_time Date is stored here flags Bitmap of following items
     * TIME_FUZZY_DATE Set if we should allow partial dates TIME_DATETIME_ONLY
     * Set if we only allow full datetimes. TIME_NO_ZERO_IN_DATE Don't allow
     * partial dates TIME_NO_ZERO_DATE Don't allow 0000-00-00 date
     * TIME_INVALID_DATES Allow 2000-02-31 status Conversion status
     *
     *
     * DESCRIPTION At least the following formats are recogniced (based on
     * number of digits) YYMMDD, YYYYMMDD, YYMMDDHHMMSS, YYYYMMDDHHMMSS
     * YY-MM-DD, YYYY-MM-DD, YY-MM-DD HH.MM.SS YYYYMMDDTHHMMSS where T is a the
     * character T (ISO8601) Also dates where all parts are zero are allowed
     *
     * The second part may have an optional .###### fraction part.
     *
     * NOTES This function should work with a format position vector as long as
     * the following things holds: - All date are kept together and all time
     * parts are kept together - Date and time parts must be separated by blank
     * - Second fractions must come after second part and be separated by a '.'.
     * (The second fractions are optional) - AM/PM must come after second
     * fractions (or after seconds if no fractions) - Year must always been
     * specified. - If time is before date, then we will use datetime format
     * only if the argument consist of two parts, separated by space. Otherwise
     * we will assume the argument is a date. - The hour part must be specified
     * in hour-minute-second order.
     *
     * status.warnings is set to: 0 Value OK MYSQL_TIME_WARN_TRUNCATED If value
     * was cut during conversion MYSQL_TIME_WARN_OUT_OF_RANGE
     * checkDate(date,flags) considers date invalid
     *
     * l_time.time_type is set as follows: MYSQL_TIMESTAMP_NONE String wasn't a
     * timestamp, like [DD [HH:[MM:[SS]]]].fraction. l_time is not changed.
     * MYSQL_TIMESTAMP_DATE DATE string (YY MM and DD parts ok)
     * MYSQL_TIMESTAMP_DATETIME Full timestamp MYSQL_TIMESTAMP_ERROR Timestamp
     * with wrong values. All elements in l_time is set to 0 RETURN VALUES 0 -
     * Ok 1 - Error
     */
    public static boolean strToDatetime(String strori, int length, MySQLTime lTime, long flags,
                                        MySQLTimeStatus status) {
        /* Skip space at start */
        String str = strori.trim();
        long fieldLength, yearLength = 0;
        long[] date = new long[MAX_DATE_PARTS];
        long addHours = 0;
        final char[] chars = str.toCharArray();
        int lastFieldPos = 0;
        int[] formatPosition;
        boolean foundDelimiter = false, foundSpace = false;
        int fracPos, fracLen;

        myTimeStatusInit(status);

        if (str.isEmpty() || !Ctype.isDigit(str.charAt(0))) {
            status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
            lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_NONE);
            return true;
        }

        boolean isInternalFormat = false;
        /*
         * This has to be changed if want to activate different timestamp
     * formats
     */
        formatPosition = INTERNAL_FORMAT_POSITIONS;

    /*
     * Calculate number of digits in first part. If length= 8 or >= 14 then
     * year is of format YYYY. (YYYY-MM-DD, YYYYMMDD, YYYYYMMDDHHMMSS)
     */
        int pos;
        int end = str.length();
        for (pos = 0; pos != end && (Ctype.isDigit(chars[pos]) || chars[pos] == 'T'); pos++) {
            //do nothing
        }

        long digits = (long) pos;
        long startLoop = 0; /* Start of scan loop */

        int[] dateLen = new int[MAX_DATE_PARTS];
        dateLen[formatPosition[0]] = 0; /* Length of year field */
        if (pos == end || chars[pos] == '.') {
    /* Found date in internal format (only numbers like YYYYMMDD) */
            yearLength = (digits == 4 || digits == 8 || digits >= 14) ? 4 : 2;
            fieldLength = yearLength;
            isInternalFormat = true;
            formatPosition = INTERNAL_FORMAT_POSITIONS;
        } else {
            if (formatPosition[0] >= 3) /* If year is after HHMMDD */ {
    /*
     * If year is not in first part then we have to determinate if
     * we got a date field or a datetime field. We do this by
     * checking if there is two numbers separated by space in the
     * input.
     */
                while (pos < end && !Ctype.spaceChar(chars[pos]))
                    pos++;
                while (pos < end && !Ctype.isDigit(chars[pos]))
                    pos++;
                if (pos == end) {
                    if ((flags & TIME_DATETIME_ONLY) != 0) {
                        status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
                        lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_NONE);
                        return true; /* Can't be a full datetime */
                    }
    /* Date field. Set hour, minutes and seconds to 0 */
                    date[0] = date[1] = date[2] = date[3] = date[4] = 0;
                    startLoop = 5; /* Start with first date part */
                }
            }

            fieldLength = formatPosition[0] == 0 ? 4 : 2;
        }

    /*
     * Only allow space in the first "part" of the datetime field and: -
     * after days, part seconds - before and after AM/PM (handled by code
     * later)
     *
     * 2003-03-03 20:00:20 AM 20:00:20.000000 AM 03-03-2000
     */
        long i = Math.max((long) formatPosition[0], (long) formatPosition[1]);
        if (i < (long) formatPosition[2])
            i = (long) formatPosition[2];
        long allowSpace = ((1 << i) | (1 << formatPosition[6]));
        allowSpace &= (1 | 2 | 4 | 8 | 64);

        long notZeroDate = 0;
        int strindex = 0;
        for (i = startLoop; i < MAX_DATE_PARTS - 1 && strindex != end && Ctype.isDigit(chars[strindex]); i++) {
            final int start = strindex;
            int tmpValue = chars[strindex++] - '0';

    /*
     * Internal format means no delimiters; every field has a fixed
     * width. Otherwise, we scan until we find a delimiter and discard
     * leading zeroes -- except for the microsecond part, where leading
     * zeroes are significant, and where we never process more than six
     * digits.
     */
            boolean scanUntilDelim = !isInternalFormat && ((i != formatPosition[6]));

            while (strindex != end && Ctype.isDigit(chars[strindex]) && (scanUntilDelim || (--fieldLength != 0))) {
                tmpValue = tmpValue * 10 + (chars[strindex] - '0');
                strindex++;
            }
            dateLen[(int) i] = (strindex - start);
            if (tmpValue > 999999) /* Impossible date part */ {
                status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
                lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_NONE);
                return true;
            }
            date[(int) i] = tmpValue;
            notZeroDate |= tmpValue;

    /* Length of next field */
            fieldLength = formatPosition[(int) i + 1] == 0 ? 4 : 2;

            if ((lastFieldPos = strindex) == end) {
                i++; /* Register last found part */
                break;
            }
    /* Allow a 'T' after day to allow CCYYMMDDT type of fields */
            if (i == formatPosition[2] && chars[strindex] == 'T') {
                strindex++; /* ISO8601: CCYYMMDDThhmmss */
                continue;
            }
            if (i == formatPosition[5]) /* Seconds */ {
                if (chars[strindex] == '.') /* Followed by part seconds */ {
                    strindex++;
    /*
     * Shift last_field_pos, so '2001-01-01 00:00:00.' is
     * treated as a valid value
     */
                    lastFieldPos = strindex;
                    fieldLength = 6; /* 6 digits */
                }
                continue;
            }
            while (strindex != end && (Ctype.isPunct(chars[strindex]) || Ctype.spaceChar(chars[strindex]))) {
                if (Ctype.spaceChar(chars[strindex])) {
                    if ((allowSpace & (1 << i)) == 0) {
                        status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
                        lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_NONE);
                        return true;
                    }
                    foundSpace = true;
                }
                strindex++;
                foundDelimiter = true; /* Should be a 'normal' date */
            }
    /* Check if next position is AM/PM */
            if (i == formatPosition[6]) /* Seconds, time for AM/PM */ {
                i++; /* Skip AM/PM part */
                if (formatPosition[7] != 255) /* If using AM/PM */ {
                    if (strindex + 2 <= end && (chars[strindex + 1] == 'M' || chars[strindex + 1] == 'm')) {
                        if (chars[strindex] == 'p' || chars[strindex] == 'P')
                            addHours = 12;
                        else if (chars[strindex] != 'a' || chars[strindex] != 'A')
                            continue; /* Not AM/PM */
                        strindex += 2; /* Skip AM/PM */
    /* Skip space after AM/PM */
                        while (strindex != end && Ctype.spaceChar(chars[strindex]))
                            strindex++;
                    }
                }
            }
            lastFieldPos = strindex;
        }
        if (foundDelimiter && !foundSpace && (flags & TIME_DATETIME_ONLY) != 0) {
            status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
            lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_NONE);
            return true; /* Can't be a datetime */
        }

        strindex = lastFieldPos;

        final long numberOfFields = i - startLoop;
        while (i < MAX_DATE_PARTS) {
            dateLen[(int) i] = 0;
            date[(int) i++] = 0;
        }

        if (!isInternalFormat) {
            yearLength = dateLen[formatPosition[0]];
            if (yearLength == 0) /* Year must be specified */ {
                status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
                lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_NONE);
                return true;
            }

            lTime.setYear(date[formatPosition[0]]);
            lTime.setMonth(date[formatPosition[1]]);
            lTime.setDay(date[formatPosition[2]]);
            lTime.setHour(date[formatPosition[3]]);
            lTime.setMinute(date[formatPosition[4]]);
            lTime.setSecond(date[formatPosition[5]]);

            fracPos = formatPosition[6];
            fracLen = dateLen[fracPos];
            status.setFractionalDigits(fracLen);
            if (fracLen < 6)
                date[fracPos] *= MySQLcom.LOG_10_INT[DATETIME_MAX_DECIMALS - fracLen];
            lTime.setSecondPart(date[fracPos]);

            if (formatPosition[7] != 255) {
                if (lTime.getHour() > 12) {
                    status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
                    lTime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
                    return true;
                }
                lTime.setHour(lTime.getHour() % 12 + addHours);
            }
        } else {
            lTime.setYear(date[0]);
            lTime.setMonth(date[1]);
            lTime.setDay(date[2]);
            lTime.setHour(date[3]);
            lTime.setMinute(date[4]);
            lTime.setSecond(date[5]);
            if (dateLen[6] < 6)
                date[6] *= MySQLcom.LOG_10_INT[DATETIME_MAX_DECIMALS - dateLen[6]];
            lTime.setSecondPart(date[6]);
            status.setFractionalDigits(dateLen[6]);
        }
        lTime.setNeg(false);

        if (yearLength == 2 && notZeroDate != 0)
            lTime.setYear(lTime.getYear() + (lTime.getYear() < YY_PART_YEAR ? 2000 : 1900));

    /*
     * Set time_type before check_datetime_range(), as the latter relies on
     * initialized time_type value.
     */
        lTime.setTimeType((numberOfFields <= 3 ?
                MySQLTimestampType.MYSQL_TIMESTAMP_DATE : MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME));

        if (numberOfFields < 3 || checkDatetimeRange(lTime)) {
    /*
     * Only give warning for a zero date if there is some garbage after
     */
            if (notZeroDate == 0) /* If zero date */ {
                for (; strindex != end; strindex++) {
                    if (!Ctype.spaceChar(chars[strindex])) {
                        notZeroDate = 1; /* Give warning */
                        break;
                    }
                }
            }
            status.setWarnings(status.getWarnings() | (notZeroDate != 0 ? MYSQL_TIME_WARN_TRUNCATED : MYSQL_TIME_WARN_ZERO_DATE));
            lTime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
            return true;
        }

        LongPtr lptmp = new LongPtr(0);
        boolean bcheckdate = checkDate(lTime, notZeroDate != 0, flags, lptmp);
        status.setWarnings((int) lptmp.get());
        if (bcheckdate) {
            lTime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
            return true;
        }

    /* Scan all digits left after microseconds */
        if (status.getFractionalDigits() == 6 && strindex != end) {
            if (Ctype.isDigit(chars[strindex])) {
    /*
     * We don't need the exact nanoseconds value. Knowing the first
     * digit is enough for rounding.
     */
                status.setNanoseconds(100 * (chars[strindex++] - '0'));
                for (; strindex != end && Ctype.isDigit(chars[strindex]); strindex++) {
                    //block
                }
            }
        }

        for (; strindex != end; strindex++) {
            if (!Ctype.spaceChar(chars[strindex])) {
                status.setWarnings(MYSQL_TIME_WARN_TRUNCATED);
                break;
            }
        }

        return false;
    }

    /*
     * Convert a time string to a MYSQL_TIME struct.
     *
     * SYNOPSIS strToTime() str A string in full TIMESTAMP format or [-] DAYS
     * [H]H:MM:SS, [H]H:MM:SS, [M]M:SS, [H]HMMSS, [M]MSS or [S]S There may be an
     * optional [.second_part] after seconds length Length of str l_time Store
     * result here status Conversion status
     *
     * status.warning is set to: MYSQL_TIME_WARN_TRUNCATED flag if the input
     * string was cut during conversion, and/or MYSQL_TIME_WARN_OUT_OF_RANGE
     * flag, if the value is out of range.
     *
     * NOTES Because of the extra days argument, this function can only work
     * with times where the time arguments are in the above order.
     *
     * RETURN 0 ok 1 error
     */
    public static boolean strToTime(String str, int length, MySQLTime lTime, MySQLTimeStatus status) {
        long[] date = new long[5];
        int pos = 0, end = length;
        final char[] chars = str.toCharArray();
        myTimeStatusInit(status);
        lTime.setNeg(false);
        for (; pos != end && Ctype.spaceChar(chars[pos]); pos++)
            length--;
        if (pos != end && chars[pos] == '-') {
            lTime.setNeg(true);
            pos++;
            length--;
        }
        if (pos == end)
            return true;

    /* Check first if this is a full TIMESTAMP */
        if (length >= 12) { /* Probably full timestamp */
            strToDatetime(str.substring(pos), length, lTime, (TIME_FUZZY_DATE | TIME_DATETIME_ONLY), status);
            if (lTime.getTimeType().compareTo(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR) >= 0)
                return lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_ERROR;
            myTimeStatusInit(status);
        }

    /* Not a timestamp. Try to get this as a DAYS_TO_SECOND string */
        long value;
        for (value = 0; pos != end && Ctype.isDigit(chars[pos]); pos++)
            value = value * 10L + (long) (chars[pos] - '0');

        if (value > LONG_MAX)
            return true;

    /* Skip all space after 'days' */
        int endOfDays = pos;
        for (; pos != end && Ctype.spaceChar(chars[pos]); pos++) {
            //do nothing
        }

        int state = 0;
        boolean gotofractional = false;
        if (end - pos > 1 && pos != endOfDays && Ctype.isDigit(chars[pos])) { /* Found days part */
            date[0] = value;
            state = 1; /* Assume next is hours */
        } else if ((end - pos) > 1 && chars[pos] == TIME_SEPARATOR && Ctype.isDigit(chars[pos + 1])) {
            date[0] = 0; /* Assume we found hours */
            date[1] = value;
            state = 2;
            pos++; /* skip ':' */
        } else {
    /* String given as one number; assume HHMMSS format */
            date[0] = 0;
            date[1] = (value / 10000);
            date[2] = (value / 100 % 100);
            date[3] = (value % 100);
            state = 4;
            gotofractional = true;
        }
        if (!gotofractional) {
    /* Read hours, minutes and seconds */
            for (; ; ) {
                for (value = 0; pos != end && Ctype.isDigit(chars[pos]); pos++)
                    value = value * 10L + (long) (chars[pos] - '0');
                date[state++] = value;
                if (state == 4 || (end - pos) < 2 || chars[pos] != TIME_SEPARATOR || !Ctype.isDigit(chars[pos + 1]))
                    break;
                pos++; /* Skip time_separator (':') */
            }

            if (state != 4) { /* Not HH:MM:SS */
                // TODO
    /*
     * Fix the date to assume that seconds was given if
     * (!found_hours && !found_days) { size_t len= sizeof(long) *
     * (state - 1); memmove((uchar*) (date+4) - len, (uchar*)
     * (date+state) - len, len); memset(date, 0,
     * sizeof(long)*(4-state)); } else memset((date+state), 0,
     * sizeof(long)*(4-state));
     */
            }
        }

    /* Get fractional second part */
        if ((end - pos) >= 2 && chars[pos] == '.' && Ctype.isDigit(chars[pos + 1])) {
            int fieldLength = 5;
            pos++;
            value = (chars[pos] - '0');
            while (++pos != end && Ctype.isDigit(chars[pos])) {
                if (fieldLength-- > 0)
                    value = value * 10 + (chars[pos] - '0');
            }
            if (fieldLength >= 0) {
                status.setFractionalDigits(DATETIME_MAX_DECIMALS - fieldLength);
                if (fieldLength > 0)
                    value *= MySQLcom.LOG_10_INT[fieldLength];
            } else {
    /* Scan digits left after microseconds */
                status.setFractionalDigits(6);
                status.setNanoseconds(100 * (chars[pos - 1] - '0'));
                for (; pos != end && Ctype.isDigit(chars[pos]); pos++) {
                    //block
                }
            }
            date[4] = value;
        } else if ((end - pos) == 1 && chars[pos] == '.') {
            pos++;
            date[4] = 0;
        } else
            date[4] = 0;

    /* Check for exponent part: E<gigit> | E<sign><digit> */
    /* (may occur as result of %g formatting of time value) */
        if ((end - pos) > 1 && (chars[pos] == 'e' || chars[pos] == 'E') &&
                (Ctype.isDigit(chars[pos + 1]) || ((chars[pos + 1] == '-' || chars[pos + 1] == '+') &&
                        (end - pos) > 2 && Ctype.isDigit(chars[pos + 2]))))
            return true;

        if (INTERNAL_FORMAT_POSITIONS[7] != 255) {
    /* Read a possible AM/PM */
            while (pos != end && Ctype.spaceChar(chars[pos]))
                pos++;
            if (pos + 2 <= end && (chars[pos + 1] == 'M' || chars[pos + 1] == 'm')) {
                if (chars[pos] == 'p' || chars[pos] == 'P') {
                    str += 2;
                    date[1] = date[1] % 12 + 12;
                } else if (chars[pos] == 'a' || chars[pos] == 'A')
                    str += 2;
            }
        }

    /* Integer overflow checks */
        if (date[0] > LONG_MAX || date[1] > LONG_MAX || date[2] > LONG_MAX || date[3] > LONG_MAX || date[4] > LONG_MAX)
            return true;

        lTime.setYear(0); /* For protocol::store_time */
        lTime.setMonth(0);

        lTime.setDay(0);
        lTime.setHour(date[1] + date[0] * 24); /* Mix days and hours */

        lTime.setMinute(date[2]);
        lTime.setSecond(date[3]);
        lTime.setSecondPart(date[4]);
        lTime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);

        if (checkTimeMmssffRange(lTime)) {
            status.setWarnings(status.getWarnings() | MYSQL_TIME_WARN_OUT_OF_RANGE);
            return true;
        }

    /* Adjust the value into supported MYSQL_TIME range */
        // adjust_time_range(l_time, &status.warnings);

    /* Check if there is garbage at end of the MYSQL_TIME specification */
        if (pos != end) {
            do {
                if (!Ctype.spaceChar(chars[pos])) {
                    status.setWarnings(status.getWarnings() | MYSQL_TIME_WARN_TRUNCATED);
                    break;
                }
            } while (++pos != end);
        }
        return false;
    }

    /*
     * Convert datetime value specified as number to broken-down TIME
     * representation and form value of DATETIME type as side-effect.
     *
     * SYNOPSIS numberToDatetime() nr - datetime value as number time_res -
     * pointer for structure for broken-down representation flags - flags to use
     * in validating date, as in strToDatetime() was_cut 0 Value ok 1 If value
     * was cut during conversion 2 checkDate(date,flags) considers date invalid
     *
     * DESCRIPTION Convert a datetime value of formats YYMMDD, YYYYMMDD,
     * YYMMDDHHMSS, YYYYMMDDHHMMSS to broken-down MYSQL_TIME representation.
     * Return value in YYYYMMDDHHMMSS format as side-effect.
     *
     * This function also checks if datetime value fits in DATETIME range.
     *
     * RETURN VALUE -1 Timestamp with wrong values anything else DATETIME as
     * integer in YYYYMMDDHHMMSS format Datetime value in YYYYMMDDHHMMSS format.
     *
     * was_cut if return value -1: one of - MYSQL_TIME_WARN_OUT_OF_RANGE -
     * MYSQL_TIME_WARN_ZERO_DATE - MYSQL_TIME_WARN_TRUNCATED otherwise 0.
     */
    public static long numberToDatetime(long nr, MySQLTime timeRes, long flags, LongPtr wasCut) {
        wasCut.set(0);
        timeRes.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);

        if (nr == 0 || nr >= 10000101000000L) {
            timeRes.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            if (nr > 99999999999999L) /* 9999-99-99 99:99:99 */ {
                wasCut.set(MYSQL_TIME_WARN_OUT_OF_RANGE);
                return -1;
            }
            return numberToDatetimeOk(nr, timeRes, flags, wasCut);
        }
        if (nr < 101) {
            wasCut.set(MYSQL_TIME_WARN_TRUNCATED);
            return -1;
        }
        if (nr <= (YY_PART_YEAR - 1) * 10000L + 1231L) {
            nr = (nr + 20000000L) * 1000000L; /* YYMMDD, year: 2000-2069 */
            return numberToDatetimeOk(nr, timeRes, flags, wasCut);
        }
        if (nr < (YY_PART_YEAR) * 10000L + 101L) {
            wasCut.set(MYSQL_TIME_WARN_TRUNCATED);
            return -1;
        }
        if (nr <= 991231L) {
            nr = (nr + 19000000L) * 1000000L; /* YYMMDD, year: 1970-1999 */
            return numberToDatetimeOk(nr, timeRes, flags, wasCut);
        }
    /*
     * Though officially we support DATE values from 1000-01-01 only, one
     * can easily insert a value like 1-1-1. So, for consistency reasons
     * such dates are allowed when TIME_FUZZY_DATE is set.
     */
        if (nr < 10000101L && (flags & TIME_FUZZY_DATE) == 0) {
            wasCut.set(MYSQL_TIME_WARN_TRUNCATED);
            return -1;
        }
        if (nr <= 99991231L) {
            nr = nr * 1000000L;
            return numberToDatetimeOk(nr, timeRes, flags, wasCut);
        }
        if (nr < 101000000L) {
            wasCut.set(MYSQL_TIME_WARN_TRUNCATED);
            return -1;
        }

        timeRes.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);

        if (nr <= (YY_PART_YEAR - 1) * (10000000000L) + (1231235959L)) {
            nr = nr + (20000000000000L); /* YYMMDDHHMMSS, 2000-2069 */
            return numberToDatetimeOk(nr, timeRes, flags, wasCut);
        }
        if (nr < YY_PART_YEAR * (10000000000L) + 101000000L) {
            wasCut.set(MYSQL_TIME_WARN_TRUNCATED);
            return -1;
        }
        if (nr <= 991231235959L)
            nr = nr + 19000000000000L; /* YYMMDDHHMMSS, 1970-1999 */
        return numberToDatetimeOk(nr, timeRes, flags, wasCut);
    }

    /**
     * helper goto ok
     *
     * @param nr
     * @param timeRes
     * @param flags
     * @param wasCut
     * @return
     */
    private static long numberToDatetimeOk(long nr, MySQLTime timeRes, long flags, LongPtr wasCut) {
        long part1 = nr / (1000000L);
        timeRes.setYear((int) (part1 / 10000L));
        part1 %= 10000L;
        timeRes.setMonth((int) part1 / 100);
        timeRes.setDay((int) part1 % 100);

        long part2 = nr - part1 * (1000000L);
        timeRes.setHour((int) (part2 / 10000L));
        part2 %= 10000L;
        timeRes.setMinute((int) part2 / 100);
        timeRes.setSecond((int) part2 % 100);

        if (!checkDatetimeRange(timeRes) && !checkDate(timeRes, (nr != 0), flags, wasCut))
            return nr;

    /* Don't want to have was_cut get set if NO_ZERO_DATE was violated. */
        if (nr == 0 && (flags & TIME_NO_ZERO_DATE) != 0)
            return -1;
        wasCut.set(MYSQL_TIME_WARN_TRUNCATED);
        return -1;
    }

    /**
     * Convert number to TIME
     *
     * @param nr       Number to convert.
     * @param ltime    ltime Variable to convert to.
     * @param warnings warnings Warning vector.
     * @retval false OK
     * @retval true No. is out of range
     */
    private static boolean numberToTime(long nr, MySQLTime ltime, LongPtr warnings) {
        if (nr > TIME_MAX_VALUE) {
    /* For huge numbers try full DATETIME, like strToTime does. */
            if (nr >= 10000000000L) /* '0001-00-00 00-00-00' */ {
                long warningsBackup = warnings.get();
                if (numberToDatetime(nr, ltime, 0, warnings) != (-1))
                    return false;
                warnings.set(warningsBackup);
            }
            setMaxTime(ltime, false);
            warnings.set(warnings.get() | MYSQL_TIME_WARN_OUT_OF_RANGE);
            return true;
        } else if (nr < -TIME_MAX_VALUE) {
            setMaxTime(ltime, true);
            warnings.set(warnings.get() | MYSQL_TIME_WARN_OUT_OF_RANGE);
            return true;
        }
        ltime.setNeg((nr < 0));
        if (ltime.isNeg()) {
            nr = -nr;
        }
        if (nr % 100 >= 60 || nr / 100 % 100 >= 60) /* Check hours and minutes */ {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
            warnings.set(warnings.get() | MYSQL_TIME_WARN_OUT_OF_RANGE);
            return true;
        }
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        ltime.setDay(0);
        ltime.setMonth(0);
        ltime.setYear(0);
        timeSetHhmmss(ltime, nr);
        ltime.setSecondPart(0);
        return false;
    }

    public static long timeToUlonglongDatetime(final MySQLTime myTime) {
        return ((myTime.getYear() * 10000L + myTime.getMonth() * 100 + myTime.getDay()) * (1000000) +
                myTime.getHour() * 10000L + myTime.getMinute() * 100L + myTime.getSecond());
    }

    public static long timeToUlonglongDate(final MySQLTime myTime) {
        return myTime.getYear() * 10000L + myTime.getMonth() * 100L + myTime.getDay();
    }

    public static long timeToUlonglongTime(final MySQLTime myTime) {
        return myTime.getHour() * 10000L + myTime.getMinute() * 100L + myTime.getSecond();
    }

    public static long timeToUlonglong(final MySQLTime myTime) {
        if (myTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME) {
            return timeToUlonglongDatetime(myTime);
        } else if (myTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE) {
            return timeToUlonglongDate(myTime);
        } else if (myTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
            return timeToUlonglongTime(myTime);
        } else if (myTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_NONE || myTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_ERROR) {
            return 0;
        } else {
            assert (false);
        }
        return 0;
    }

    public static long myPackedTimeGetIntPart(long x) {
        return x >> 24;
    }

    public static long myPackedTimeGetFracPart(long x) {
        return ((x) % (1L << 24));
    }

    public static long myPackedTimeMake(long i, long f) {
        return (i << 24) + f;
    }

    public static long myPackedTimeMakeInt(long i) {
        return i << 24;
    }

    /**
     * Convert year to packed numeric date representation. Packed value for YYYY
     * is the same to packed value for date YYYY-00-00.
     */
    public static long yearToLonglongDatetimePacked(long year) {
        long ymd = ((year * 13) << 5);
        return myPackedTimeMakeInt(ymd << 17);
    }

    /**
     * Convert datetime to packed numeric datetime representation.
     *
     * @param ltime The value to convert.
     * @return Packed numeric representation of ltime.
     */
    public static long timeToLonglongDatetimePacked(final MySQLTime ltime) {
        long ymd = ((ltime.getYear() * 13 + ltime.getMonth()) << 5) | ltime.getDay();
        long hms = (ltime.getHour() << 12) | (ltime.getMinute() << 6) | ltime.getSecond();
        long tmp = myPackedTimeMake(((ymd << 17) | hms), ltime.getSecondPart());
        return ltime.isNeg() ? -tmp : tmp;
    }

    /**
     * Convert date to packed numeric date representation. Numeric packed date
     * format is similar to numeric packed datetime representation, with zero
     * hhmmss part.
     *
     * @param ltime The value to convert.
     * @return Packed numeric representation of ltime.
     */
    public static long timeToLonglongDatePacked(final MySQLTime ltime) {
        long ymd = ((ltime.getYear() * 13 + ltime.getMonth()) << 5) | ltime.getDay();
        return myPackedTimeMakeInt(ymd << 17);
    }

    /**
     * Convert time value to numeric packed representation.
     *
     * @param ltime The value to convert.
     * @return Numeric packed representation.
     */
    public static long timeToLonglongTimePacked(final MySQLTime ltime) {
    /* If month is 0, we mix day with hours: "1 00:10:10" . "24:00:10" */
        long hms = (((ltime.getMonth() != 0 ? 0 : ltime.getDay() * 24) + ltime.getHour()) << 12) | (ltime.getMinute() << 6) | ltime.getSecond();
        long tmp = myPackedTimeMake(hms, ltime.getSecondPart());
        return ltime.isNeg() ? -tmp : tmp;
    }

    /**
     * Convert a temporal value to packed numeric temporal representation,
     * depending on its time_type.
     *
     * @return Packed numeric time/date/datetime representation.
     * @ltime The value to convert.
     */
    public static long timeToLonglongPacked(final MySQLTime ltime) {
        if (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE) {
            return timeToLonglongDatePacked(ltime);
        } else if (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME) {
            return timeToLonglongDatetimePacked(ltime);
        } else if (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
            return timeToLonglongTimePacked(ltime);
        } else if (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_NONE || ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_ERROR) {
            return 0;
        }
        assert (false);
        return 0;
    }

    public static long timeToLonglongPacked(final MySQLTime ltime, FieldTypes type) {
        if (type == FieldTypes.MYSQL_TYPE_TIME) {
            return timeToLonglongTimePacked(ltime);
        } else if (type == FieldTypes.MYSQL_TYPE_DATETIME || type == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            return timeToLonglongDatetimePacked(ltime);
        } else if (type == FieldTypes.MYSQL_TYPE_DATE) {
            return timeToLonglongDatePacked(ltime);
        } else {
            return timeToLonglongPacked(ltime);
        }
    }

    /**
     * Convert packed numeric datetime representation to MYSQL_TIME.
     *
     * @param ltime ltime The datetime variable to convert to.
     * @param tmp   The packed numeric datetime value.
     */
    public static void timeFromLonglongDatetimePacked(MySQLTime ltime, long tmp) {
        ltime.setNeg((tmp < 0));
        if (ltime.isNeg()) {
            tmp = -tmp;
        }

        ltime.setSecondPart(myPackedTimeGetFracPart(tmp));
        long ymdhms = myPackedTimeGetIntPart(tmp);

        long ymd = ymdhms >> 17;
        long ym = ymd >> 5;

        ltime.setDay(ymd % (1 << 5));
        ltime.setMonth(ym % 13);
        ltime.setYear(ym / 13);

        long hms = ymdhms % (1 << 17);
        ltime.setSecond(hms % (1 << 6));
        ltime.setMinute((hms >> 6) % (1 << 6));
        ltime.setHour((hms >> 12));

        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
    }

    /**
     * Convert packed numeric date representation to MYSQL_TIME.
     *
     * @param ltime ltime The date variable to convert to.
     * @param tmp   The packed numeric date value.
     */
    public static void timeFromLonglongDatePacked(MySQLTime ltime, long tmp) {
        timeFromLonglongDatetimePacked(ltime, tmp);
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);
    }

    /**
     * Convert time packed numeric representation to time.
     *
     * @param ltime The MYSQL_TIME variable to set.
     * @param tmp   The packed numeric representation.
     */
    public static void timeFromLonglongTimePacked(MySQLTime ltime, long tmp) {
        long hms;
        ltime.setNeg((tmp < 0));
        if (ltime.isNeg()) {
            tmp = -tmp;
        }
        hms = myPackedTimeGetIntPart(tmp);
        ltime.setYear(0);
        ltime.setMonth(0);
        ltime.setDay(0);
        ltime.setHour((hms >> 12) % (1 << 10)); /* 10 bits starting at 12th */
        ltime.setMinute((hms >> 6) % (1 << 6)); /* 6 bits starting at 6th */
        ltime.setSecond(hms % (1 << 6)); /* 6 bits starting at 0th */
        ltime.setSecondPart(myPackedTimeGetFracPart(tmp));
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
    }

    /**
     * Set day, month and year from a number
     *
     * @param ltime  MYSQL_TIME variable
     * @param yymmdd Number in YYYYMMDD format
     */
    public static void timeSetYymmdd(MySQLTime ltime, long yymmdd) {
        ltime.setDay((int) (yymmdd % 100));
        ltime.setMonth((int) (yymmdd / 100) % 100);
        ltime.setYear((int) (yymmdd / 10000));
    }

    /**
     * Set hour, minute and secondr from a number
     *
     * @param ltime  MYSQL_TIME variable
     * @param hhmmss Number in HHMMSS format
     */
    public static void timeSetHhmmss(MySQLTime ltime, long hhmmss) {
        ltime.setSecond((int) (hhmmss % 100));
        ltime.setMinute((int) (hhmmss / 100) % 100);
        ltime.setHour((int) (hhmmss / 10000));
    }

    /*
     * Calculate nr of day since year 0 in new date-system (from 1615)
     *
     * SYNOPSIS calc_daynr() year Year (exact 4 digit year, no year conversions)
     * month Month day Day
     *
     * NOTES: 0000-00-00 is a valid date, and will return 0
     *
     * RETURN Days since 0000-00-00
     */

    public static long calcDaynr(long year, long month, long day) {
        long delsum;
        int y = (int) year; /* may be < 0 temporarily */

        if (y == 0 && month == 0)
            return 0; /* Skip errors */
    /* Cast to int to be able to handle month == 0 */
        delsum = (long) (365 * y + 31 * ((int) month - 1) + (int) day);
        if (month <= 2)
            y--;
        else
            delsum -= (long) ((int) month * 4 + 23) / 10;
        int temp = (y / 100 + 1) * 3 / 4;
        assert (delsum + y / 4 - temp >= 0);
        return (delsum + y / 4 - temp);
    } /* calc_daynr */
    /* Calc days in one year. works with 0 <= year <= 99 */

    public static int calcDaysInYear(int year) {
        return ((year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) ? 366 : 365);
    }

    public static long calcDaysInYear(long year) {
        return ((year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) ? 366 : 365);
    }

    public static long weekMode(int mode) {
        int weekFormat = (mode & 7);
        if ((weekFormat & WEEK_MONDAY_FIRST) == 0)
            weekFormat ^= WEEK_FIRST_WEEKDAY;
        return weekFormat;
    }

    public static String myTimeToStrL(MySQLTime lTime, long dec) {
        String stime = String.format("%s%02d:%02d:%02d", (lTime.isNeg() ? "-" : ""), lTime.getHour(), lTime.getMinute(),
                lTime.getSecond());
        if (dec != 0) {
            // TODO: 6 digits after Decimal point
            // String stmp = String.format("%06d", l_time.second_part);
            // stime += "." + stmp.substring(0, (int) dec);
        }
        return stime;
    }

    public static String myDateToStr(MySQLTime mysqlTime) {
        return String.format("%04d-%02d-%02d", mysqlTime.getYear(), mysqlTime.getMonth(), mysqlTime.getDay());
    }

    /**
     * Print a datetime value with an optional fractional part.
     *
     * @return The length of the result string.
     * @l_time The MYSQL_TIME value to print.
     * @to OUT The string pointer to print at.
     */
    public static String myDatetimeToStr(final MySQLTime lTime, long dec) {
        StringPtr ptrtmp = new StringPtr("");
        timeToDatetimeStr(ptrtmp, lTime);
        String res = ptrtmp.get();
        if (dec != 0) {
            // TODO: 6 digits after Decimal point
            // String stmp = String.format("%06d", l_time.second_part);
            // res += "." + stmp.substring(0, (int) dec);
        }
        return res;
    }

    /*
     * Convert struct DATE/TIME/DATETIME value to string using built-in MySQL
     * time conversion formats.
     *
     * SYNOPSIS my_TIME_to_string()
     *
     * NOTE The string must have at least MAX_DATE_STRING_REP_LENGTH bytes
     * reserved.
     */

    public static String myTimeToStr(final MySQLTime lTime, int dec) {
        if (lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME) {
            return myDatetimeToStr(lTime, dec);
        } else if (lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE) {
            return myDateToStr(lTime);
        } else if (lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
            return myTimeToStrL(lTime, dec);
        } else if (lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_NONE || lTime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_ERROR) {
            return null;
        } else {
            return null;
        }
    }

    /*-------------------------My_time.h  end------------------------------*/
    /*-------------------------the other type change to time type----------------------------*/

    /* Rounding functions */

    private static final long[] MSEC_ROUND_ADD = new long[]{500000000, 50000000, 5000000, 500000, 50000, 5000, 0};

    /**
     * Convert decimal value to datetime value with a warning.
     *
     * @param decimal The value to convert from.
     * @param flags   Conversion flags.
     * @return False on success, true on error.
     * @param[out] ltime The variable to convert to.
     */
    public static boolean myDecimalToDatetimeWithWarn(BigDecimal decimal, MySQLTime ltime, long flags) {
        LongPtr warnings = new LongPtr(0);
        String sbd = decimal.toString();
        String[] sbds = sbd.split("\\.");
        long intPart = Long.parseLong(sbds[0]);
        long secondPart = 0;
        if (sbds.length == 2)
            secondPart = Long.parseLong(sbds[1]);
        if (numberToDatetime(intPart, ltime, flags, warnings) == -1) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
            return true;
        } else if (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE) {
            /**
             * Generate a warning in case of DATE with fractional part:
             * 20011231.1234 . '2001-12-31' unless the caller does not want the
             * warning: for example, CAST does.
             */
            if (secondPart != 0 && (flags & TIME_NO_DATE_FRAC_WARN) == 0) {
                warnings.set(warnings.get() | MYSQL_TIME_WARN_TRUNCATED);
            }
        } else if ((flags & TIME_NO_NSEC_ROUNDING) == 0) {
            ltime.setSecondPart(secondPart);
        }
        return false;
    }

    /**
     * Round time value to the given precision.
     *
     * @param ltime The value to round.
     * @param dec   Precision.
     * @return False on success, true on error.
     */
    public static boolean myTimeRound(MySQLTime ltime, int dec) {
    /* Add half away from zero */
        boolean rc = timeAddNanosecondsWithRound(ltime, MSEC_ROUND_ADD[dec]);
    /* Truncate non-significant digits */
        myTimeTrunc(ltime, dec);
        return rc;
    }

    /**
     * Round datetime value to the given precision.
     *
     * @param ltime ltime The value to round.
     * @param dec   Precision.
     * @return False on success, true on error.
     */
    public static boolean myDatetimeRound(MySQLTime ltime, int dec) {
        assert (dec <= DATETIME_MAX_DECIMALS);
    /* Add half away from zero */
        boolean rc = datetimeAddNanosecondsWithRound(ltime, MSEC_ROUND_ADD[dec]);
    /* Truncate non-significant digits */
        myTimeTrunc(ltime, dec);
        return rc;
    }

    public static boolean dateAddInterval(MySQLTime ltime, MySqlIntervalUnit intType, Interval interval) {
        long period, sign;

        ltime.setNeg(false);

        sign = (interval.isNeg() ? -1 : 1);

        if (intType == MySqlIntervalUnit.SECOND ||
                intType == MySqlIntervalUnit.SECOND_MICROSECOND ||
                intType == MySqlIntervalUnit.MICROSECOND ||
                intType == MySqlIntervalUnit.MINUTE ||
                intType == MySqlIntervalUnit.HOUR ||
                intType == MySqlIntervalUnit.MINUTE_MICROSECOND ||
                intType == MySqlIntervalUnit.MINUTE_SECOND ||
                intType == MySqlIntervalUnit.HOUR_MICROSECOND ||
                intType == MySqlIntervalUnit.HOUR_SECOND ||
                intType == MySqlIntervalUnit.HOUR_MINUTE ||
                intType == MySqlIntervalUnit.DAY_MICROSECOND ||
                intType == MySqlIntervalUnit.DAY_SECOND ||
                intType == MySqlIntervalUnit.DAY_MINUTE ||
                intType == MySqlIntervalUnit.DAY_HOUR) {
            long microseconds, extraSec;
            ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME); // Return
            // full
            // date
            microseconds = ltime.getSecondPart() + sign * interval.getSecondPart();
            extraSec = microseconds / 1000000L;
            microseconds = microseconds % 1000000L;

            long sec = ((ltime.getDay() - 1) * 3600 * 24L + ltime.getHour() * 3600 + ltime.getMinute() * 60 + ltime.getSecond() +
                    sign * (interval.getDay() * 3600 * 24L + interval.getHour() * 3600 + interval.getMinute() * (60) + interval.getSecond())) +
                    extraSec;
            if (microseconds < 0) {
                microseconds += (1000000L);
                sec--;
            }
            long days = sec / (3600 * (24));
            sec -= days * 3600 * (24);
            if (sec < 0) {
                days--;
                sec += 3600 * 24;
            }
            ltime.setSecondPart(microseconds);
            ltime.setSecond((sec % 60));
            ltime.setMinute((sec / 60 % 60));
            ltime.setHour((sec / 3600));
            long daynr = calcDaynr(ltime.getYear(), ltime.getMonth(), 1) + days;
    /* Day number from year 0 to 9999-12-31 */
            if (daynr > MAX_DAY_NUMBER)
                return true;
            LongPtr ptrYear = new LongPtr(ltime.getYear());
            LongPtr ptrMonth = new LongPtr(ltime.getMonth());
            LongPtr ptrDay = new LongPtr(ltime.getDay());
            getDateFromDaynr(daynr, ptrYear, ptrMonth, ptrDay);
            ltime.setYear(ptrYear.get());
            ltime.setMonth(ptrMonth.get());
            ltime.setDay(ptrDay.get());
        } else if (intType == MySqlIntervalUnit.DAY || intType == MySqlIntervalUnit.WEEK) {
            period = (calcDaynr(ltime.getYear(), ltime.getMonth(), ltime.getDay()) + sign * interval.getDay());
    /* Daynumber from year 0 to 9999-12-31 */
            if (period > MAX_DAY_NUMBER)
                return true;
            LongPtr ptrYear = new LongPtr(ltime.getYear());
            LongPtr ptrMonth = new LongPtr(ltime.getMonth());
            LongPtr ptrDay = new LongPtr(ltime.getDay());
            getDateFromDaynr(period, ptrYear, ptrMonth, ptrDay);
            ltime.setYear(ptrYear.get());
            ltime.setMonth(ptrMonth.get());
            ltime.setDay(ptrDay.get());

        } else if (intType == MySqlIntervalUnit.YEAR) {
            ltime.setYear(ltime.getYear() + sign * interval.getYear());
            if (ltime.getYear() >= 10000)
                return true;
            if (ltime.getMonth() == 2 && ltime.getDay() == 29 && calcDaysInYear(ltime.getYear()) != 366)
                ltime.setDay(28); // Was leap-year

        } else if (intType == MySqlIntervalUnit.YEAR_MONTH || intType == MySqlIntervalUnit.QUARTER || intType == MySqlIntervalUnit.MONTH) {
            period = (ltime.getYear() * 12 + sign * interval.getYear() * 12 + ltime.getMonth() - 1 + sign * interval.getMonth());
            if (period >= 120000L)
                return true;
            ltime.setYear((period / 12));
            ltime.setMonth((period % 12L) + 1);
    /* Adjust day if the new month doesn't have enough days */
            if (ltime.getDay() > DAYS_IN_MONTH[(int) ltime.getMonth() - 1]) {
                ltime.setDay(DAYS_IN_MONTH[(int) ltime.getMonth() - 1]);
                if (ltime.getMonth() == 2 && calcDaysInYear(ltime.getYear()) == 366)
                    ltime.setDay(ltime.getDay() + 1); // Leap-year
            }

        } else {
            return true;
        }

        return false; // Ok

    }

    /**
     * Convert double value to datetime value with a warning.
     *
     * @param db    The value to convert from.
     * @param flags Conversion flags.
     * @return False on success, true on error.
     * @param[out] ltime The variable to convert to.
     */
    public static boolean myDoubleToDatetimeWithWarn(double db, MySQLTime ltime, long flags) {
        LongPtr warnings = new LongPtr(0);
        String sbd = String.valueOf(db);
        String[] sbds = sbd.split("\\.");
        long intPart = Long.parseLong(sbds[0]);
        long secondPart = 0;
        if (sbds.length == 2)
            secondPart = Long.parseLong(sbds[1]);
        if (numberToDatetime(intPart, ltime, flags, warnings) == -1) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
            return true;
        } else if (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE) {
            /**
             * Generate a warning in case of DATE with fractional part:
             * 20011231.1234 . '2001-12-31' unless the caller does not want the
             * warning: for example, CAST does.
             */
            if (secondPart != 0 && (flags & TIME_NO_DATE_FRAC_WARN) == 0) {
                warnings.set(warnings.get() | MYSQL_TIME_WARN_TRUNCATED);
            }
        } else if ((flags & TIME_NO_NSEC_ROUNDING) == 0) {
            ltime.setSecondPart(secondPart);
        }
        return false;
    }

    /**
     * Convert longlong value to datetime value with a warning.
     *
     * @param nr The value to convert from.
     * @return False on success, true on error.
     * @param[out] ltime The variable to convert to.
     */
    public static boolean myLonglongToDatetimeWithWarn(long nr, MySQLTime ltime, long flags) {
        LongPtr warning = new LongPtr(0);
        return numberToDatetime(nr, ltime, flags, warning) == -1;
    }

    public static boolean strToDatetimeWithWarn(String str, MySQLTime ltime, long flags) {
        return strToDatetime(str, str.length(), ltime, flags, new MySQLTimeStatus());
    }

    public static boolean myDecimalToTimeWithWarn(BigDecimal decimal, MySQLTime ltime) {
        LongPtr warnings = new LongPtr(0);
        String sbd = decimal.toString();
        String[] sbds = sbd.split("\\.");
        long intPart = Long.parseLong(sbds[0]);
        long secondPart = 0;
        if (sbds.length == 2)
            secondPart = Long.parseLong(sbds[1]);
        if (numberToTime(intPart, ltime, warnings)) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
            return true;
        }
        ltime.setSecondPart(secondPart);
        return false;
    }

    public static boolean myDoubleToTimeWithWarn(double db, MySQLTime ltime) {
        LongPtr warnings = new LongPtr(0);
        String sbd = String.valueOf(db);
        String[] sbds = sbd.split("\\.");
        long intPart = Long.parseLong(sbds[0]);
        long secondPart = 0;
        if (sbds.length == 2)
            secondPart = Long.parseLong(sbds[1]);
        if (numberToTime(intPart, ltime, warnings)) {
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);
            return true;
        }
        ltime.setSecondPart(secondPart);
        return false;
    }

    public static boolean myLonglongToTimeWithWarn(long nr, MySQLTime ltime) {
        LongPtr warning = new LongPtr(0);
        return numberToTime(nr, ltime, warning);
    }

    public static boolean strToTimeWithWarn(String str, MySQLTime ltime) {
        return strToTime(str, str.length(), ltime, new MySQLTimeStatus());
    }

    /**
     * Convert time to datetime.
     * <p>
     * The time value is added to the current datetime value.
     *
     * @param ltime  Time value to convert from.
     * @param ltime2 Datetime value to convert to.
     */
    public static void timeToDatetime(final MySQLTime ltime, MySQLTime ltime2) {
        java.util.Calendar cal1 = ltime.toCalendar();
        java.util.Calendar cal2 = java.util.Calendar.getInstance();
        cal2.clear();
        cal2.setTimeInMillis(cal1.getTimeInMillis());
        ltime2.setYear(cal2.get(java.util.Calendar.YEAR));
        ltime2.setMonth(cal2.get(java.util.Calendar.MONTH) + 1);
        ltime.setDay(cal2.get(java.util.Calendar.DAY_OF_MONTH));
        ltime2.setHour(cal2.get(java.util.Calendar.HOUR_OF_DAY));
        ltime2.setMinute(cal2.get(java.util.Calendar.MINUTE));
        ltime2.setSecond(cal2.get(java.util.Calendar.SECOND));
        ltime2.setSecondPart(cal2.get(java.util.Calendar.MILLISECOND) * 1000);
    }

    public static void datetimeToTime(MySQLTime ltime) {
        ltime.setDay(0);
        ltime.setMonth(0);
        ltime.setYear(0);
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
    }

    public static void datetimeToDate(MySQLTime ltime) {
        ltime.setSecondPart(0);
        ltime.setSecond(0);
        ltime.setMinute(0);
        ltime.setHour(0);
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);
    }

    public static void dateToDatetime(MySQLTime ltime) {
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
    }

    public static long timeToUlonglongDatetimeRound(final MySQLTime ltime) {
        // Catch simple cases
        if (ltime.getSecondPart() < 500000)
            return timeToUlonglongDatetime(ltime);
        if (ltime.getSecond() < 59)
            return timeToUlonglongDatetime(ltime) + 1;
        return timeToUlonglongDatetime(ltime); // TIME_microseconds_round(ltime);
    }

    public static long timeToUlonglongTimeRound(final MySQLTime ltime) {
        if (ltime.getSecondPart() < 500000)
            return timeToUlonglongTime(ltime);
        if (ltime.getSecond() < 59)
            return timeToUlonglongTime(ltime) + 1;
        // Corner case e.g. 'hh:mm:59.5'. Proceed with slower method.
        return timeToUlonglongTime(ltime);
    }

    public static double timeToDoubleDatetime(final MySQLTime ltime) {
        return (double) timeToUlonglongDatetime(ltime) + timeMicroseconds(ltime);
    }

    public static double timeToDoubleTime(final MySQLTime ltime) {
        return (double) timeToUlonglongTime(ltime) + timeMicroseconds(ltime);
    }

    public static double timeToDouble(final MySQLTime ltime) {
        return (double) timeToUlonglong(ltime) + timeMicroseconds(ltime);
    }

    /**
     * Convert a datetime from broken-down MYSQL_TIME representation to
     * corresponding TIMESTAMP value.
     *
     * @param - current thread
     * @param t - datetime in broken-down representation,
     * @param - pointer to bool which is set to true if t represents value
     *          which doesn't exists (falls into the spring time-gap) or to
     *          false otherwise.
     * @return
     * @retval Number seconds in UTC since start of Unix Epoch corresponding to
     * t.
     * @retval 0 - t contains datetime value which is out of TIMESTAMP range.
     */
    public static long timeToTimestamp(final MySQLTime t) {
        long timestamp = t.toCalendar().getTimeInMillis() / 1000;

        return timestamp;
    }

    public static void timeFromLonglongPacked(MySQLTime ltime, FieldTypes type, long packedValue) {
        if (type == FieldTypes.MYSQL_TYPE_TIME) {
            timeFromLonglongTimePacked(ltime, packedValue);

        } else if (type == FieldTypes.MYSQL_TYPE_DATE) {
            timeFromLonglongDatePacked(ltime, packedValue);

        } else if (type == FieldTypes.MYSQL_TYPE_DATETIME || type == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            timeFromLonglongDatetimePacked(ltime, packedValue);

        } else {
            assert (false);
            ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_ERROR);

        }
    }

    /**
     * Unpack packed numeric temporal value to date/time value and then convert
     * to decimal representation.
     *
     * @param type        MySQL field type.
     * @param packedValue Packed numeric temporal representation.
     * @return A decimal value in on of the following formats, depending on
     * type: YYYYMMDD, hhmmss.ffffff or YYMMDDhhmmss.ffffff.
     */
    public static BigDecimal myDecimalFromDatetimePacked(FieldTypes type, long packedValue) {
        MySQLTime ltime = new MySQLTime();
        if (type == FieldTypes.MYSQL_TYPE_TIME) {
            timeFromLonglongTimePacked(ltime, packedValue);
            return time2MyDecimal(ltime);
        } else if (type == FieldTypes.MYSQL_TYPE_DATE) {
            timeFromLonglongDatePacked(ltime, packedValue);
            return ulonglong2decimal(timeToUlonglongDate(ltime));
        } else if (type == FieldTypes.MYSQL_TYPE_DATETIME || type == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            timeFromLonglongDatetimePacked(ltime, packedValue);
            return date2MyDecimal(ltime);
        } else {
            assert (false);
            return ulonglong2decimal(0);
        }
    }

    public static long longlongFromDatetimePacked(FieldTypes type, long packedValue) {
        MySQLTime ltime = new MySQLTime();
        if (type == FieldTypes.MYSQL_TYPE_TIME) {
            timeFromLonglongTimePacked(ltime, packedValue);
            return timeToUlonglongTime(ltime);
        } else if (type == FieldTypes.MYSQL_TYPE_DATE) {
            timeFromLonglongDatePacked(ltime, packedValue);
            return timeToUlonglongDate(ltime);
        } else if (type == FieldTypes.MYSQL_TYPE_DATETIME || type == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            timeFromLonglongDatetimePacked(ltime, packedValue);
            return timeToUlonglongDatetime(ltime);
        } else {
            return 0;
        }
    }

    public static double doubleFromDatetimePacked(FieldTypes type, long packedValue) {
        long result = longlongFromDatetimePacked(type, packedValue);
        return result + ((double) myPackedTimeGetFracPart(packedValue)) / 1000000;
    }

    /**
     * Convert time value to my_decimal in format hhmmss.ffffff
     *
     * @param ltime Date value to convert from.
     */
    public static BigDecimal time2MyDecimal(final MySQLTime ltime) {
        String stmp = String.format("%02d%02d%02d.%06d", ltime.getHour(), ltime.getMinute(), ltime.getSecond(), ltime.getSecondPart());
        return new BigDecimal(stmp);
    }

    /**
     * Convert datetime value to my_decimal in format YYYYMMDDhhmmss.ffffff
     *
     * @param ltime Date value to convert from.
     */
    public static BigDecimal date2MyDecimal(final MySQLTime ltime) {
        String stmp = String.format("%04d%02d%02d%02d%02d%02d.%06d", ltime.getYear(), ltime.getMonth(), ltime.getDay(), ltime.getHour(),
                ltime.getMinute(), ltime.getSecond(), ltime.getSecondPart());
        return new BigDecimal(stmp);
    }
    /* Functions to handle periods */

    public static long convertPeriodToMonth(long period) {
        long a, b;
        if (period == 0)
            return 0L;
        if ((a = period / 100) < YY_PART_YEAR)
            a += 2000;
        else if (a < 100)
            a += 1900;
        b = period % 100;
        return a * 12 + b - 1;
    }

    public static long convertMonthToPeriod(long month) {
        long year;
        if (month == 0L)
            return 0L;
        if ((year = month / 12) < 100) {
            year += (year < YY_PART_YEAR) ? 2000 : 1900;
        }
        return year * 100 + month % 12 + 1;
    }
    /* Calc weekday from daynr */

    /* Returns 0 for monday, 1 for tuesday .... */

    public static int calcWeekday(long daynr, boolean sundayFirstDayOfWeek) {
        return ((int) ((daynr + 5L + (sundayFirstDayOfWeek ? 1L : 0L)) % 7));
    }

    /*
     * The bits in week_format has the following meaning: WEEK_MONDAY_FIRST (0)
     * If not set Sunday is first day of week If set Monday is first day of week
     * WEEK_YEAR (1) If not set Week is in range 0-53
     *
     * Week 0 is returned for the the last week of the previous year (for a date
     * at start of january) In this case one can get 53 for the first week of
     * next year. This flag ensures that the week is relevant for the given
     * year. Note that this flag is only releveant if WEEK_JANUARY is not set.
     *
     * If set Week is in range 1-53.
     *
     * In this case one may get week 53 for a date in January (when the week is
     * that last week of previous year) and week 1 for a date in December.
     *
     * WEEK_FIRST_WEEKDAY (2) If not set Weeks are numbered according to ISO
     * 8601:1988 If set The week that contains the first 'first-day-of-week' is
     * week 1.
     *
     * ISO 8601:1988 means that if the week containing January 1 has four or
     * more days in the new year, then it is week 1; Otherwise it is the last
     * week of the previous year, and the next week is week 1.
     */

    public static long calcWeek(MySQLTime lTime, long weekBehaviour, LongPtr year) {
        long days;
        long daynr = calcDaynr(lTime.getYear(), lTime.getMonth(), lTime.getDay());
        long firstDaynr = calcDaynr(lTime.getYear(), 1, 1);
        boolean mondayFirst = (weekBehaviour & WEEK_MONDAY_FIRST) != 0;
        boolean weekYear = (weekBehaviour & WEEK_YEAR) != 0;
        boolean firstWeekday = (weekBehaviour & WEEK_FIRST_WEEKDAY) != 0;

        long weekday = calcWeekday(firstDaynr, !mondayFirst);
        year.set(lTime.getYear());

        if (lTime.getMonth() == 1 && lTime.getDay() <= 7 - weekday) {
            if (!weekYear && ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)))
                return 0;
            weekYear = true;
            year.decre();
            firstDaynr -= (days = calcDaysInYear(year.get()));
            weekday = (weekday + 53 * 7 - days) % 7;
        }

        if ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4))
            days = daynr - (firstDaynr + (7 - weekday));
        else
            days = daynr - (firstDaynr - weekday);

        if (weekYear && days >= 52 * 7) {
            weekday = (weekday + calcDaysInYear(year.get())) % 7;
            if ((!firstWeekday && weekday < 4) || (firstWeekday && weekday == 0)) {
                year.incre();
                return 1;
            }
        }
        return days / 7 + 1;
    }

    /**
     * Convert a datetime MYSQL_TIME representation to corresponding
     * "struct timeval" value.
     * <p>
     * Things like '0000-01-01', '2000-00-01', '2000-01-00' (i.e. incomplete
     * date) return error.
     * <p>
     * Things like '0000-00-00 10:30:30' or '0000-00-00 00:00:00.123456' (i.e.
     * empty date with non-empty time) return error.
     * <p>
     * Zero datetime '0000-00-00 00:00:00.000000' is allowed and is mapper to
     * {tv_sec=0, tv_usec=0}.
     * <p>
     * Note: In case of error, tm value is not initialized.
     * <p>
     * Note: "warnings" is not initialized to zero, so new warnings are added to
     * the old ones. Caller must make sure to initialize "warnings".
     *
     * @return
     * @param[in] thd current thd
     * @param[in] ltime datetime value
     * @param[out] tm timeval value
     * @param[out] warnings pointer to warnings vector
     * @retval false on success
     * @retval true on error
     */
    public static boolean datetimeToTimeval(final MySQLTime ltime, Timeval tm) {
        return checkDate(ltime, ltime.isNonZeroDate(), TIME_NO_ZERO_IN_DATE, new LongPtr(0)) ||
                datetimeWithNoZeroInDateToTimeval(ltime, tm);
    }
    /* Change a daynr to year, month and day */

    /* Daynr 0 is returned as date 00.00.00 */

    public static void getDateFromDaynr(long daynr, LongPtr retYear, LongPtr retMonth, LongPtr retDay) {
        int year, temp, leapDay, dayOfYear, daysInYear;
        int monthPos;
        if (daynr <= 365L || daynr >= 3652500) { /* Fix if wrong daynr */
            retYear.set(0);
            retMonth.set(0);
            retDay.set(0);
        } else {
            year = (int) (daynr * 100 / 36525L);
            temp = (((year - 1) / 100 + 1) * 3) / 4;
            dayOfYear = (int) (daynr - (long) year * 365L) - (year - 1) / 4 + temp;
            while (dayOfYear > (daysInYear = calcDaysInYear(year))) {
                dayOfYear -= daysInYear;
                (year)++;
            }
            leapDay = 0;
            if (daysInYear == 366) {
                if (dayOfYear > 31 + 28) {
                    dayOfYear--;
                    if (dayOfYear == 31 + 28)
                        leapDay = 1; /* Handle leapyears leapday */
                }
            }
            retMonth.set(1);
            for (monthPos = 0; dayOfYear > DAYS_IN_MONTH[monthPos]; dayOfYear -= DAYS_IN_MONTH[monthPos++], retMonth.incre()) {
                //do nothing
            }
            retYear.set(year);
            retDay.set(dayOfYear + leapDay);
        }
    }
    /*-----------------------helper method---------------------------*/

    private static boolean checkTimeMmssffRange(final MySQLTime ltime) {
        return ltime.getMinute() >= 60 || ltime.getSecond() >= 60 || ltime.getSecondPart() > 999999;
    }

    private static void setMaxTime(MySQLTime tm, boolean neg) {
        tm.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        setMaxHhmmss(tm);
        tm.setNeg(neg);
    }

    /**
     * Set hour, minute and second of a MYSQL_TIME variable to maximum time
     * value. Unlike set_max_time(), does not touch the other structure members.
     */
    private static void setMaxHhmmss(MySQLTime tm) {
        tm.setHour(TIME_MAX_HOUR);
        tm.setMinute(TIME_MAX_MINUTE);
        tm.setSecond(TIME_MAX_SECOND);
    }

    private static double timeMicroseconds(final MySQLTime ltime) {
        return (double) ltime.getSecondPart() / 1000000;
    }

    private static BigDecimal ulonglong2decimal(long from) {
        BigInteger bi = MySQLcom.getUnsignedLong(from);
        return new BigDecimal(bi);
    }

    private static int timeToDatetimeStr(StringPtr ptr, final MySQLTime ltime) {
        char[] res = new char[19];
        int index = 0;
        long temp;
    /* Year */
        temp = ltime.getYear() / 100;
        res[index++] = (char) ('0' + temp / 10);
        res[index++] = (char) ('0' + temp % 10);
        temp = ltime.getYear() % 100;
        res[index++] = (char) ('0' + temp / 10);
        res[index++] = (char) ('0' + temp % 10);
        res[index++] = '-';
    /* Month */
        temp = ltime.getMonth();
        long temp2 = temp / 10;
        temp = temp - temp2 * 10;
        res[index++] = (char) ('0' + (char) (temp2));
        res[index++] = (char) ('0' + (char) (temp));
        res[index++] = '-';
    /* Day */
        temp = ltime.getDay();
        temp2 = temp / 10;
        temp = temp - temp2 * 10;
        res[index++] = (char) ('0' + (char) (temp2));
        res[index++] = (char) ('0' + (char) (temp));
        res[index++] = ' ';
    /* Hour */
        temp = ltime.getHour();
        temp2 = temp / 10;
        temp = temp - temp2 * 10;
        res[index++] = (char) ('0' + (char) (temp2));
        res[index++] = (char) ('0' + (char) (temp));
        res[index++] = ':';
    /* Minute */
        temp = ltime.getMinute();
        temp2 = temp / 10;
        temp = temp - temp2 * 10;
        res[index++] = (char) ('0' + (char) (temp2));
        res[index++] = (char) ('0' + (char) (temp));
        res[index++] = ':';
    /* Second */
        temp = ltime.getSecond();
        temp2 = temp / 10;
        temp = temp - temp2 * 10;
        res[index++] = (char) ('0' + (char) (temp2));
        res[index++] = (char) ('0' + (char) (temp));
        ptr.set(new String(res));
        return 19;
    }

    /**
     * Check datetime, date, or normalized time (i.e. time without days) range.
     *
     * @param ltime Datetime value.
     * @returns
     * @retval FALSE on success
     * @retval TRUE on error
     */
    private static boolean checkDatetimeRange(MySQLTime ltime) {
    /*
     * In case of MYSQL_TIMESTAMP_TIME hour value can be up to
     * TIME_MAX_HOUR. In case of MYSQL_TIMESTAMP_DATETIME it cannot be
     * bigger than 23.
     */
        return ltime.getYear() > 9999 || ltime.getMonth() > 12 || ltime.getDay() > 31 || ltime.getMinute() > 59 || ltime.getSecond() > 59 ||
                ltime.getSecondPart() > 999999 ||
                (ltime.getHour() > (ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ? TIME_MAX_HOUR : 23));
    }

    /* Calc days in one year. works with 0 <= year <= 99 */

    private static void myTimeStatusInit(MySQLTimeStatus status) {
        status.setWarnings(0);
        status.setNanoseconds(0);
        status.setFractionalDigits(0);
    }

    /**
     * Convert a datetime MYSQL_TIME representation to corresponding
     * "struct timeval" value.
     * <p>
     * ltime must previously be checked for TIME_NO_ZERO_IN_DATE. Things like
     * '0000-01-01', '2000-00-01', '2000-01-00' are not allowed and asserted.
     * <p>
     * Things like '0000-00-00 10:30:30' or '0000-00-00 00:00:00.123456' (i.e.
     * empty date with non-empty time) return error.
     * <p>
     * Zero datetime '0000-00-00 00:00:00.000000' is allowed and is mapper to
     * {tv_sec=0, tv_usec=0}.
     * <p>
     * Note: In case of error, tm value is not initialized.
     * <p>
     * Note: "warnings" is not initialized to zero, so new warnings are added to
     * the old ones. Caller must make sure to initialize "warnings".
     *
     * @return
     * @param[in] thd current thd
     * @param[in] ltime datetime value
     * @param[out] tm timeval value
     * @param[out] warnings pointer to warnings vector
     * @retval false on success
     * @retval true on error
     */
    private static boolean datetimeWithNoZeroInDateToTimeval(final MySQLTime ltime, Timeval tm) {
        if (ltime.getMonth() == 0) /* Zero date */ {
            assert (ltime.getYear() == 0 && ltime.getDay() == 0);
            if (ltime.isNonZeroTime()) {
    /*
     * Return error for zero date with non-zero time, e.g.:
     * '0000-00-00 10:20:30' or '0000-00-00 00:00:00.123456'
     */
                return true;
            }
            tm.setTvUsec(0);
            tm.setTvSec(0); // '0000-00-00 00:00:00.000000'
            return false;
        }

        tm.setTvSec(timeToTimestamp(ltime));
        tm.setTvUsec(ltime.getSecondPart());
        return false;
    }

    private static boolean timeAddNanosecondsWithRound(MySQLTime ltime, long nanoseconds) {
    /* We expect correct input data */
        assert (nanoseconds < 1000000000);
        assert (!checkTimeMmssffRange(ltime));

        if (nanoseconds < 500)
            return false;

        ltime.setSecondPart(ltime.getSecondPart() + (nanoseconds + 500) / 1000);
        if (ltime.getSecondPart() < 1000000)
            return false;

        ltime.setSecondPart(ltime.getSecondPart() % 1000000);
        if (ltime.getSecond() < 59) {
            ltime.setSecond(ltime.getSecond() + 1);
            return false;
        }

        ltime.setSecond(0);
        if (ltime.getMinute() < 59) {
            ltime.setMinute(ltime.getMinute() + 1);
            return false;
        }
        ltime.setMinute(0);
        ltime.setHour(ltime.getHour() + 1);

    /*
     * We can get '838:59:59.000001' at this point, which is bigger than the
     * maximum possible value '838:59:59.000000'. Checking only "hour > 838"
     * is not enough. Do full adjust_time_range().
     */
        // adjust_time_range(ltime, warnings);
        return false;
    }

    /**
     * Add nanoseconds to a datetime value with rounding.
     *
     * @param ltime       MYSQL_TIME variable to add to.
     * @param nanoseconds Nanosecons value.
     * @retval False on success, true on error.
     */
    private static boolean datetimeAddNanosecondsWithRound(MySQLTime ltime, long nanoseconds) {
        assert (nanoseconds < 1000000000);
        if (nanoseconds < 500)
            return false;

        ltime.setSecondPart(ltime.getSecondPart() + (nanoseconds + 500) / 1000);
        if (ltime.getSecondPart() < 1000000)
            return false;

        ltime.setSecondPart(ltime.getSecondPart() % 1000000);
        Interval interval = new Interval();
        interval.setSecond(1);

        return dateAddInterval(ltime, MySqlIntervalUnit.SECOND, interval);
    }

    private static void myTimeTrunc(MySQLTime ltime, int decimals) {
        ltime.setSecondPart(ltime.getSecondPart() - myTimeFractionRemainder(ltime.getSecondPart(), decimals));
    }

    private static long myTimeFractionRemainder(long nr, int decimals) {
        return nr % (long) MySQLcom.LOG_10_INT[DATETIME_MAX_DECIMALS - decimals];
    }

    /*
     * Calculate difference between two datetime values as seconds
     * microseconds.
     *
     * SYNOPSIS calc_time_diff() l_time1 - TIME/DATE/DATETIME value l_time2 -
     * TIME/DATE/DATETIME value seconds_out - Out parameter where difference
     * between l_time1 and l_time2 in seconds is stored. microseconds_out- Out
     * parameter where microsecond part of difference between l_time1 and
     * l_time2 is stored.
     *
     * NOTE This function calculates difference between l_time1 and l_time2
     * absolute values. So one should set l_sign and correct result if he want
     * to take signs into account (i.e. for MYSQL_TIME values).
     *
     * RETURN VALUES Returns sign of difference. 1 means negative result 0 means
     * positive result
     */
    public static boolean calcTimeDiff(MySQLTime lTime1, MySQLTime lTime2, int lSign, LongPtr secondsOut,
                                       LongPtr microsecondsOut) {
        long days;
        boolean neg;
        long microseconds;

    /*
     * We suppose that if first argument is MYSQL_TIMESTAMP_TIME the second
     * argument should be TIMESTAMP_TIME also. We should check it before
     * calc_time_diff call.
     */
        if (lTime1.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) // Time
            // value
            days = (long) lTime1.getDay() - lSign * (long) lTime2.getDay();
        else {
            days = calcDaynr(lTime1.getYear(), lTime1.getMonth(), lTime1.getDay());
            if (lTime2.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME)
                days -= lSign * (long) lTime2.getDay();
            else
                days -= lSign * calcDaynr(lTime2.getYear(), lTime2.getMonth(), lTime2.getDay());
        }

        microseconds = ((long) days * SECONDS_IN_24H +
                (long) (lTime1.getHour() * 3600L + lTime1.getMinute() * 60L + lTime1.getSecond()) -
                lSign * (long) (lTime2.getHour() * 3600L + lTime2.getMinute() * 60L + lTime2.getSecond())) * (1000000) +
                (long) lTime1.getSecondPart() - lSign * (long) lTime2.getSecondPart();

        neg = false;
        if (microseconds < 0) {
            microseconds = -microseconds;
            neg = true;
        }
        secondsOut.set(microseconds / 1000000L);
        microsecondsOut.set((long) (microseconds % 1000000L));
        return neg;

        // boolean ret = false;
        // try {
        // Calendar cal1 = l_time1.toCalendar();
        // Calendar cal2 = l_time2.toCalendar();
        // long milsecond1 = cal1.getTimeInMillis();
        // long milsecond2 = cal2.getTimeInMillis();
        // if (milsecond1 < milsecond2)
        // ret = true;
        // long abs = Math.abs(milsecond1 - milsecond2);
        // seconds_out.set(abs / 1000);
        // microseconds_out.set(abs % 1000 * 1000);
        // return ret;
        // } catch (Exception e) {
        // return ret;
        // }
    }

    public static void calcTimeFromSec(MySQLTime to, long seconds, long microseconds) {
        // to.neg is not cleared, it may already be set to a useful value
        to.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        to.setYear(0);
        to.setMonth(0);
        to.setDay(0);
        to.setHour((long) (seconds / 3600L));
        long tSeconds = (long) (seconds % 3600L);
        to.setMinute(tSeconds / 60L);
        to.setSecond(tSeconds % 60L);
        to.setSecondPart(microseconds);
    }

    /**
     * Convert a string to a interval value.
     * <p>
     * To make code easy, allow interval objects without separators.
     */

    public static boolean getIntervalValue(Item arg, MySqlIntervalUnit unit, StringPtr strValue,
                                           Interval interval) {
        long[] array = new long[5];
        long value = 0;
        //        int int_type = unit.ordinal();

        if (unit == MySqlIntervalUnit.SECOND && arg.getDecimals() != 0) {
            BigDecimal decimalValue = arg.valDecimal();
            if (decimalValue == null)
                return false;

            boolean neg = decimalValue.compareTo(BigDecimal.ZERO) < 0;
            if (!neg) {
                interval.setNeg(false);
                interval.setSecond(decimalValue.longValue());
                interval.setSecondPart((long) ((decimalValue.doubleValue() - interval.getSecond()) * 1000000));
            } else {
                interval.setNeg(true);
                interval.setSecond(-decimalValue.longValue());
                interval.setSecondPart((long) ((-decimalValue.doubleValue() - interval.getSecond()) * 1000000));
            }
            return false;
        } else if (unit == MySqlIntervalUnit.YEAR || unit == MySqlIntervalUnit.QUARTER ||
                unit == MySqlIntervalUnit.MONTH || unit == MySqlIntervalUnit.WEEK || unit == MySqlIntervalUnit.DAY ||
                unit == MySqlIntervalUnit.HOUR || unit == MySqlIntervalUnit.MINUTE ||
                unit == MySqlIntervalUnit.SECOND || unit == MySqlIntervalUnit.MICROSECOND) {
            value = arg.valInt().longValue();
            if (arg.isNullValue())
                return true;
            if (value < 0) {
                interval.setNeg(true);
                value = -value;
            }
        }

        BoolPtr negPtr = new BoolPtr(interval.isNeg());
        if (unit == MySqlIntervalUnit.YEAR) {
            interval.setYear(value);

        } else if (unit == MySqlIntervalUnit.QUARTER) {
            interval.setMonth((value * 3));

        } else if (unit == MySqlIntervalUnit.MONTH) {
            interval.setMonth(value);

        } else if (unit == MySqlIntervalUnit.WEEK) {
            interval.setDay((value * 7));

        } else if (unit == MySqlIntervalUnit.DAY) {
            interval.setDay(value);

        } else if (unit == MySqlIntervalUnit.HOUR) {
            interval.setHour(value);

        } else if (unit == MySqlIntervalUnit.MINUTE) {
            interval.setMinute(value);

        } else if (unit == MySqlIntervalUnit.SECOND) {
            interval.setSecond(value);

        } else if (unit == MySqlIntervalUnit.MICROSECOND) {
            interval.setSecondPart(value);

        } else if (unit == MySqlIntervalUnit.YEAR_MONTH) {
            if (getIntervalInfo(arg, strValue, negPtr, 2, array, false))
                return true;
            interval.setYear(array[0]);
            interval.setMonth(array[1]);

        } else if (unit == MySqlIntervalUnit.DAY_HOUR) {
            if (getIntervalInfo(arg, strValue, negPtr, 2, array, false))
                return true;
            interval.setDay(array[0]);
            interval.setHour(array[1]);

        } else if (unit == MySqlIntervalUnit.DAY_MINUTE) {
            if (getIntervalInfo(arg, strValue, negPtr, 3, array, false))
                return true;
            interval.setDay(array[0]);
            interval.setHour(array[1]);
            interval.setMinute(array[2]);

        } else if (unit == MySqlIntervalUnit.DAY_SECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 4, array, false))
                return true;
            interval.setDay(array[0]);
            interval.setHour(array[1]);
            interval.setMinute(array[2]);
            interval.setSecond(array[3]);

        } else if (unit == MySqlIntervalUnit.HOUR_MINUTE) {
            if (getIntervalInfo(arg, strValue, negPtr, 2, array, false))
                return true;
            interval.setHour(array[0]);
            interval.setMinute(array[1]);

        } else if (unit == MySqlIntervalUnit.HOUR_SECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 3, array, false))
                return true;
            interval.setHour(array[0]);
            interval.setMinute(array[1]);
            interval.setSecond(array[2]);

        } else if (unit == MySqlIntervalUnit.MINUTE_SECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 2, array, false))
                return true;
            interval.setMinute(array[0]);
            interval.setSecond(array[1]);

        } else if (unit == MySqlIntervalUnit.DAY_MICROSECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 5, array, true))
                return true;
            interval.setDay(array[0]);
            interval.setHour(array[1]);
            interval.setMinute(array[2]);
            interval.setSecond(array[3]);
            interval.setSecondPart(array[4]);

        } else if (unit == MySqlIntervalUnit.HOUR_MICROSECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 4, array, true))
                return true;
            interval.setHour(array[0]);
            interval.setMinute(array[1]);
            interval.setSecond(array[2]);
            interval.setSecondPart(array[3]);

        } else if (unit == MySqlIntervalUnit.MINUTE_MICROSECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 3, array, true))
                return true;
            interval.setMinute(array[0]);
            interval.setSecond(array[1]);
            interval.setSecondPart(array[2]);

        } else if (unit == MySqlIntervalUnit.SECOND_MICROSECOND) {
            if (getIntervalInfo(arg, strValue, negPtr, 2, array, true))
                return true;
            interval.setSecond(array[0]);
            interval.setSecondPart(array[1]);

        }
        interval.setNeg(negPtr.get());
        return false;
    }

    /**
     * @param args           item expression which we convert to an ASCII string
     * @param strValue       string buffer
     * @param negPtr         set to true if interval is prefixed by '-'
     * @param count:         count of elements in result array
     * @param values:        array of results
     * @param transformMsec: if value is true we suppose that the last part of string value
     *                       is microseconds and we should transform value to six digit
     *                       value. For example, '1.1' . '1.100000'
     * @details Get a array of positive numbers from a string object. Each
     * number is separated by 1 non digit character Return error if
     * there is too many numbers. If there is too few numbers, assume
     * that the numbers are left out from the high end. This allows one
     * to give: DAY_TO_SECOND as "D MM:HH:SS", "MM:HH:SS" "HH:SS" or as
     * seconds.
     */
    public static boolean getIntervalInfo(Item args, StringPtr strValue, BoolPtr negPtr, int count, long[] values,
                                          boolean transformMsec) {
        String res = args.valStr();
        strValue.set(res);
        if (res == null)
            return true;

        char[] cs = res.toCharArray();
        int str = 0;
        int end = cs.length;

        if (str < end && cs[str] == '-') {
            negPtr.set(true);
            str++;
        }

        while (str < end && !Ctype.isDigit(cs[str]))
            str++;

        long msecLength = 0;
        for (int i = 0; i < count; i++) {
            long value;
            int start = str;
            for (value = 0; str != end && Ctype.isDigit(cs[str]); str++)
                value = value * 10 + (cs[str] - '0');
            msecLength = 6 - (str - start);
            values[i] = value;
            while (str != end && !Ctype.isDigit(cs[str]))
                str++;
            if (str == end && i != count - 1) {
                i++;
    /* Change values[0...i-1] . values[0...count-1] */
                // FIXME
                break;
            }
        }

        if (transformMsec && msecLength > 0)
            values[count - 1] *= (long) MySQLcom.pow10((int) msecLength);

        return (str != end);
    }

    public static boolean secToTime(LLDivT seconds, MySQLTime ltime) {
        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);

        if (seconds.getQuot() < 0 || seconds.getRem() < 0) {
            ltime.setNeg(true);
            seconds.setQuot(-seconds.getQuot());
            seconds.setRem(-seconds.getRem());
        }

        if (seconds.getQuot() > TIME_MAX_VALUE_SECONDS) {
            setMaxHhmmss(ltime);
            return true;
        }

        ltime.setHour((seconds.getQuot() / 3600));
        int sec = (int) (seconds.getQuot() % 3600);
        ltime.setMinute(sec / 60);
        ltime.setSecond(sec % 60);
        boolean warning = timeAddNanosecondsWithRound(ltime, seconds.getRem());
        return warning;
    }

    /**
     * Extract datetime value to MYSQL_TIME struct from string value according
     * to format string.
     *
     * @param format              date/time format specification
     * @param valStr              String to decode
     * @param lTime               Store result here
     * @param cachedTimestampType It uses to get an appropriate warning in the case when the
     *                            value is truncated.
     * @return 1 error
     * @note Possibility to parse strings matching to patterns equivalent to
     * compound specifiers is mainly intended for use from inside of this
     * function in order to understand %T and %r conversion specifiers, so
     * number of conversion specifiers that can be used in such
     * sub-patterns is limited. Also most of checks are skipped in this
     * case.
     * @note If one adds new format specifiers to this function he should also
     * consider adding them to Item_func_str_to_date::fix_from_format().
     */
    public static boolean extractDateTime(DateTimeFormat format, String valStr, MySQLTime lTime,
                                          MySQLTimestampType cachedTimestampType, String dateTimeType) {
        int weekday = 0, yearday = 0, daypart = 0;
        int weekNumber = -1;
        BoolPtr error = new BoolPtr(false);
        int strictWeekNumberYear = -1;
        int fracPart;
        boolean usaTime = false;
        boolean sundayFirstNFirstWeekNonIso = false;
        boolean strictWeekNumber = false;
        boolean strictWeekNumberYearType = false;
        int val = 0;
        int valEnd = valStr.length();
        char[] valcs = valStr.toCharArray();
        int ptr = 0;
        int end = format.getFormat().length();
        char[] ptrcs = format.getFormat().toCharArray();

        for (; ptr != end && val != valEnd; ptr++) {

            if (ptrcs[ptr] == '%' && ptr + 1 != end) {
                int valLen;
                int tmp;

                error.set(false);

                valLen = valEnd - val;
                switch (ptrcs[++ptr]) {
    /* Year */
                    case 'Y':
                        tmp = val + Math.min(4, valLen);
                        lTime.setYear(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        if (tmp - val <= 2)
                            lTime.setYear(MyTime.year2000Handling(lTime.getYear()));
                        val = tmp;
                        break;
                    case 'y':
                        tmp = val + Math.min(2, valLen);
                        lTime.setYear(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        val = tmp;
                        lTime.setYear(MyTime.year2000Handling(lTime.getYear()));
                        break;

                    /* Month */
                    case 'm':
                    case 'c':
                        tmp = val + Math.min(2, valLen);
                        lTime.setMonth(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        val = tmp;
                        break;
                    case 'M':
                        lTime.setMonth(MySQLcom.checkWord(MONTH_NAMES, valcs, val, valEnd));
                        if (lTime.getMonth() <= 0) {
                            // logger.warn
                            return true;
                        }
                        break;
                    case 'b':
                        lTime.setMonth(MySQLcom.checkWord(AB_MONTH_NAMES, valcs, val, valEnd));
                        if (lTime.getMonth() <= 0) {
                            // logger.warn
                            return true;
                        }
                        break;
    /* Day */
                    case 'd':
                    case 'e':
                        tmp = val + Math.min(2, valLen);
                        lTime.setDay(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        val = tmp;
                        break;
                    case 'D':
                        tmp = val + Math.min(2, valLen);
                        lTime.setDay(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
    /* Skip 'st, 'nd, 'th .. */
                        val = tmp + Math.min((int) (valEnd - tmp), 2);
                        break;

    /* Hour */
                    case 'h':
                    case 'I':
                    case 'l':
                        usaTime = true;
    /* fall through */
                    case 'k':
                    case 'H':
                        tmp = val + Math.min(2, valLen);
                        lTime.setHour(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        val = tmp;
                        break;

    /* Minute */
                    case 'i':
                        tmp = val + Math.min(2, valLen);
                        lTime.setMinute(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        val = tmp;
                        break;

    /* Second */
                    case 's':
                    case 'S':
                        tmp = val + Math.min(2, valLen);
                        lTime.setSecond(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        val = tmp;
                        break;

    /* Second part */
                    case 'f':
                        tmp = valEnd;
                        if (tmp - val > 6)
                            tmp = val + 6;
                        lTime.setSecondPart(MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue());
                        fracPart = 6 - (int) (tmp - val);
                        if (fracPart > 0)
                            lTime.setSecondPart(lTime.getSecondPart() * MySQLcom.LOG_10_INT[fracPart]);
                        val = tmp;
                        break;

    /* AM / PM */
                    case 'p':
                        if (valLen < 2 || !usaTime) {
                            // logger.warn
                            return true;
                        }
                        if (new String(valcs, val, 2).compareTo("PM") == 0)
                            daypart = 12;
                        else if (new String(valcs, val, 2).compareTo("AM") == 0) {
                            {
                                // logger.warn
                                return true;
                            }
                        }
                        val += 2;
                        break;

    /* Exotic things */
                    case 'W':
                        if ((weekday = MySQLcom.checkWord(DAY_NAMES, valcs, val, valEnd)) <= 0) {
                            {
                                // logger.warn
                                return true;
                            }
                        }
                        break;
                    case 'a':
                        if ((weekday = MySQLcom.checkWord(AB_DAY_NAMES, valcs, val, valEnd)) <= 0) {
                            {
                                // logger.warn
                                return true;
                            }
                        }
                        break;
                    case 'w':
                        tmp = val + 1;
                        if ((weekday = MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue()) < 0 || weekday >= 7) {
                            {
                                // logger.warn
                                return true;
                            }
                        }
    /* We should use the same 1 - 7 scale for %w as for %W */
                        if (weekday == 0)
                            weekday = 7;
                        val = tmp;
                        break;
                    case 'j':
                        tmp = val + Math.min(valLen, 3);
                        yearday = MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue();
                        val = tmp;
                        break;

    /* Week numbers */
                    case 'V':
                    case 'U':
                    case 'v':
                    case 'u':
                        sundayFirstNFirstWeekNonIso = (ptrcs[ptr] == 'U' || ptrcs[ptr] == 'V');
                        strictWeekNumber = (ptrcs[ptr] == 'V' || ptrcs[ptr] == 'v');
                        tmp = val + Math.min(valLen, 2);
                        if ((weekNumber = MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue()) < 0 ||
                                (strictWeekNumber && weekNumber == 0) || weekNumber > 53) {
                            {
                                // logger.warn
                                return true;
                            }
                        }
                        val = tmp;
                        break;

    /* Year used with 'strict' %V and %v week numbers */
                    case 'X':
                    case 'x':
                        strictWeekNumberYearType = (ptrcs[ptr] == 'X');
                        tmp = val + Math.min(4, valLen);
                        strictWeekNumberYear = MySQLcom.myStrtoll10(valcs, val, tmp, error).intValue();
                        val = tmp;
                        break;

    /* Time in AM/PM notation */
                    case 'r':
    /*
     * We can't just set error here, as we don't want to
     * generate two warnings in case of errors
     */
                        if (extractDateTime(TIME_AMPM_FORMAT, new String(valcs, val, valEnd - val), lTime,
                                cachedTimestampType, "time"))
                            return true;
                        break;

    /* Time in 24-hour notation */
                    case 'T':
                        if (extractDateTime(TIME_24_HRS_FORMAT, new String(valcs, val, valEnd - val), lTime,
                                cachedTimestampType, "time"))
                            return true;
                        break;

    /* Conversion specifiers that match classes of characters */
                    case '.':
                        while (val < valEnd && Ctype.isPunct(valcs[val]))
                            val++;
                        break;
                    case '@':
                        while (val < valEnd && Ctype.myIsAlpha(valcs[val]))
                            val++;
                        break;
                    case '#':
                        while (val < valEnd && Ctype.isDigit(valcs[val]))
                            val++;
                        break;
                    default: {
                        // logger.warn
                        return true;
                    }
                }
                if (error.get()) { // Error from MySql_com.myStrtoll10
                    // logger.warn
                    return true;
                }
            } else if (!Ctype.spaceChar(ptrcs[ptr])) {
                if (valcs[val] != ptrcs[ptr]) {
                    // logger.warn
                    return true;
                }
                val++;
            }
        }
        if (usaTime) {
            if (lTime.getHour() > 12 || lTime.getHour() < 1) {
                // logger.warn
                return true;
            }
            lTime.setHour(lTime.getHour() % 12 + daypart);
        }

        if (yearday > 0) {
            long days;
            days = calcDaynr(lTime.getYear(), 1L, 1L) + yearday - 1;
            if (days <= 0 || days > MAX_DAY_NUMBER) {
                // logger.warn
                return true;
            }
            LongPtr yPtr = new LongPtr(lTime.getYear());
            LongPtr mPtr = new LongPtr(lTime.getMonth());
            LongPtr dPtr = new LongPtr(lTime.getDay());
            getDateFromDaynr(days, yPtr, mPtr, dPtr);
            lTime.setYear(yPtr.get());
            lTime.setMonth(mPtr.get());
            lTime.setDay(dPtr.get());
        }

        if (weekNumber >= 0 && weekday != 0) {
            int days;
            long weekdayB;

    /*
     * %V,%v require %X,%x resprectively, %U,%u should be used with %Y
     * and not %X or %x
     */
            if ((strictWeekNumber && (strictWeekNumberYear < 0 ||
                    strictWeekNumberYearType != sundayFirstNFirstWeekNonIso)) ||
                    (!strictWeekNumber && strictWeekNumberYear >= 0)) {
                // logger.warn
                return true;
            }

    /* Number of days since year 0 till 1st Jan of this year */
            days = (int) calcDaynr((strictWeekNumber ? strictWeekNumberYear : lTime.getYear()), 1, 1);
    /* Which day of week is 1st Jan of this year */
            weekdayB = calcWeekday(days, sundayFirstNFirstWeekNonIso);

    /*
     * Below we are going to sum: 1) number of days since year 0 till
     * 1st day of 1st week of this year 2) number of days between 1st
     * week and our week 3) and position of our day in the week
     */
            if (sundayFirstNFirstWeekNonIso) {
                days += ((weekdayB == 0) ? 0 : 7) - weekdayB + (weekNumber - 1) * 7 + weekday % 7;
            } else {
                days += ((weekdayB <= 3) ? 0 : 7) - weekdayB + (weekNumber - 1) * 7 + (weekday - 1);
            }

            if (days <= 0 || days > MAX_DAY_NUMBER) {
                // logger.warn
                return true;
            }
            LongPtr yPtr = new LongPtr(lTime.getYear());
            LongPtr mPtr = new LongPtr(lTime.getMonth());
            LongPtr dPtr = new LongPtr(lTime.getDay());
            getDateFromDaynr(days, yPtr, mPtr, dPtr);
            lTime.setYear(yPtr.get());
            lTime.setMonth(mPtr.get());
            lTime.setDay(dPtr.get());
        }

        if (lTime.getMonth() > 12 || lTime.getDay() > 31 || lTime.getHour() > 23 || lTime.getMinute() > 59 || lTime.getSecond() > 59) {
            // logger.warn
            return true;
        }

        if (val != valEnd) {
            do {
                if (!Ctype.spaceChar(valcs[val])) {
                    // TS-TODO: extract_date_time is not UCS2 safe
                    //
                    break;
                }
            } while (++val != valEnd);
        }
        return false;

    }

    /**
     * Create a formated date/time value in a string.
     *
     * @return
     */
    public static boolean makeDateTime(DateTimeFormat format, MySQLTime lTime, MySQLTimestampType type,
                                       StringPtr strPtr) {
        int hoursI;
        int weekday;
        int ptr = 0, end;
        char[] formatChars = format.getFormat().toCharArray();
        StringBuilder str = new StringBuilder();
        MyLocale locale = MyLocales.MY_LOCALE_EN_US;

        if (lTime.isNeg())
            str.append('-');

        end = format.getFormat().length();
        for (; ptr != end; ptr++) {
            if (formatChars[ptr] != '%' || ptr + 1 == end)
                str.append(formatChars[ptr]);
            else {
                switch (formatChars[++ptr]) {
                    case 'M':
                        if (lTime.getMonth() == 0) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        str.append(locale.getMonthNames().getTypeNames()[(int) (lTime.getMonth() - 1)]);

                        break;
                    case 'b':
                        if (lTime.getMonth() == 0) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        str.append(locale.getAbMonthNames().getTypeNames()[(int) (lTime.getMonth() - 1)]);
                        break;
                    case 'W':
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ||
                                (lTime.getMonth() == 0 && lTime.getYear() == 0)) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        weekday = MyTime.calcWeekday(MyTime.calcDaynr(lTime.getYear(), lTime.getMonth(), lTime.getDay()), false);
                        str.append(locale.getDayNames().getTypeNames()[weekday]);
                        break;
                    case 'a':
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ||
                                (lTime.getMonth() == 0 && lTime.getYear() == 0)) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        weekday = MyTime.calcWeekday(MyTime.calcDaynr(lTime.getYear(), lTime.getMonth(), lTime.getDay()), false);
                        str.append(locale.getAbDayNames().getTypeNames()[weekday]);
                        break;
                    case 'D':
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        str.append(String.format("%01d", lTime.getDay()));
                        if (lTime.getDay() >= 10 && lTime.getDay() <= 19)
                            str.append("th");
                        else {
                            int tmp = (int) (lTime.getDay() % 10);
                            switch (tmp) {
                                case 1:
                                    str.append("st");
                                    break;
                                case 2:
                                    str.append("nd");
                                    break;
                                case 3:
                                    str.append("rd");
                                    break;
                                default:
                                    str.append("th");
                                    break;
                            }
                        }
                        break;
                    case 'Y':
                        str.append(String.format("%04d", lTime.getYear()));
                        break;
                    case 'y':
                        str.append(String.format("%02d", lTime.getYear()));
                        break;
                    case 'm':
                        str.append(String.format("%02d", lTime.getMonth()));
                        break;
                    case 'c':
                        str.append(String.format("%01d", lTime.getMonth()));
                        break;
                    case 'd':
                        str.append(String.format("%02d", lTime.getDay()));
                        break;
                    case 'e':
                        str.append(String.format("%01d", lTime.getDay()));
                        break;
                    case 'f':
                        str.append(String.format("%06d", lTime.getSecondPart()));
                        break;
                    case 'H':
                        str.append(String.format("%02d", lTime.getHour()));
                        break;
                    case 'h':
                    case 'I':
                        hoursI = (int) ((lTime.getHour() % 24 + 11) % 12 + 1);
                        str.append(String.format("%02d", hoursI));
                        break;
                    case 'i': /* minutes */
                        str.append(String.format("%02d", lTime.getMinute()));
                        break;
                    case 'j':
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        str.append(String.format("%03d", MyTime.calcDaynr(lTime.getYear(), lTime.getMonth(), lTime.getDay()) -
                                MyTime.calcDaynr(lTime.getYear(), 1, 1) + 1));
                        break;
                    case 'k':
                        str.append(String.format("%01d", lTime.getHour()));
                        break;
                    case 'l':
                        hoursI = (int) ((lTime.getHour() % 24 + 11) % 12 + 1);
                        str.append(String.format("%01d", lTime.getHour()));
                        break;
                    case 'p':
                        hoursI = (int) (lTime.getHour() % 24);
                        str.append(hoursI < 12 ? "AM" : "PM");
                        break;
                    case 'r':
                        String tmpFmt = ((lTime.getHour() % 24) < 12) ? "%02d:%02d:%02d AM" : "%02d:%02d:%02d PM";
                        str.append(String.format(tmpFmt, (lTime.getHour() + 11) % 12 + 1, lTime.getMinute(), lTime.getSecond()));
                        break;
                    case 'S':
                    case 's':
                        str.append(String.format("%02d", lTime.getSecond()));
                        break;
                    case 'T':
                        str.append(String.format("%02d:%02d:%02d", lTime.getHour(), lTime.getMinute(), lTime.getSecond()));
                        break;
                    case 'U':
                    case 'u': {
                        LongPtr year = new LongPtr(0);
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        str.append(String.format("%02d", MyTime.calcWeek(lTime,
                                formatChars[ptr] == 'U' ? MyTime.WEEK_FIRST_WEEKDAY : MyTime.WEEK_MONDAY_FIRST, year)));
                    }
                    break;
                    case 'v':
                    case 'V': {
                        LongPtr year = new LongPtr(0);
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        str.append(String.format("%02d",
                                MyTime.calcWeek(lTime,
                                        formatChars[ptr] == 'V' ? (MyTime.WEEK_YEAR | MyTime.WEEK_FIRST_WEEKDAY) :
                                                (MyTime.WEEK_YEAR | MyTime.WEEK_MONDAY_FIRST),
                                        year)));
                    }
                    break;
                    case 'x':
                    case 'X': {
                        LongPtr year = new LongPtr(0);
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        MyTime.calcWeek(lTime, formatChars[ptr] == 'X' ? MyTime.WEEK_YEAR | MyTime.WEEK_FIRST_WEEKDAY :
                                MyTime.WEEK_YEAR | MyTime.WEEK_MONDAY_FIRST, year);
                        str.append(String.format("%04d", year.get()));
                    }
                    break;
                    case 'w':
                        if (type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ||
                                (lTime.getMonth() == 0 && lTime.getYear() == 0)) {
                            strPtr.set(str.toString());
                            return true;
                        }
                        weekday = MyTime.calcWeekday(MyTime.calcDaynr(lTime.getYear(), lTime.getMonth(), lTime.getDay()), true);
                        str.append(String.format("%01d", weekday));
                        break;

                    default:
                        str.append(format.getFormat().substring(ptr));
                        break;
                }
            }
        }
        strPtr.set(str.toString());
        return false;
    }
}
