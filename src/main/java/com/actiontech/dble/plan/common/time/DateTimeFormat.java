/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.time;

public class DateTimeFormat {
    //public char time_separator; /* Separator between hour and minute */
    //public int flag; /* For future */
    private String format;

    public DateTimeFormat() {

    }

    //public DateTimeFormat(char time_separator, int flag, String format) {
    //    this.time_separator = time_separator;
    //    this.flag = flag;
    //    this.format = format;
    //}
    public DateTimeFormat(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
