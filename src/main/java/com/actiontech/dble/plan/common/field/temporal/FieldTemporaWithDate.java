/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.temporal;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

/**
 * Abstract class for types with date with optional time, with or without
 * fractional part: DATE, DATETIME, DATETIME(N), TIMESTAMP, TIMESTAMP(N).
 *
 * @author ActionTech
 */
public abstract class FieldTemporaWithDate extends FieldTemporal {

    public FieldTemporaWithDate(String name, String table, int charsetIndex, int fieldLength, int decimals,
                                long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public long valTimeTemporal() {
        internalJob();
        return isNull() ? 0 : MyTime.timeToLonglongTimePacked(ltime);
    }

    @Override
    public long valDateTemporal() {
        internalJob();
        return isNull() ? 0 : MyTime.timeToLonglongDatetimePacked(ltime);
    }

    @Override
    public boolean getTime(MySQLTime time) {
        internalJob();
        return isNull() || getDate(time, MyTime.TIME_FUZZY_DATE);
    }

    @Override
    public int compare(byte[] v1, byte[] v2) {
        if (v1 == null && v2 == null)
            return 0;
        else if (v1 == null) {
            return -1;
        } else if (v2 == null) {
            return 1;
        } else
            try {
                String sval1 = MySQLcom.getFullString(charsetName, v1);
                String sval2 = MySQLcom.getFullString(charsetName, v2);
                MySQLTime ltime1 = new MySQLTime();
                MySQLTime ltime2 = new MySQLTime();
                MyTime.strToDatetimeWithWarn(sval1, ltime1, MyTime.TIME_FUZZY_DATE);
                MyTime.strToDatetimeWithWarn(sval2, ltime2, MyTime.TIME_FUZZY_DATE);
                return ltime1.getCompareResult(ltime2);
            } catch (Exception e) {
                LOGGER.info("String to biginteger exception!", e);
                return -1;
            }
    }
}
