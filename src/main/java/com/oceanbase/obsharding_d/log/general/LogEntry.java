/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.general;

public class LogEntry {

    protected long time;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public LogEntry() {
        this.time = System.currentTimeMillis();
    }

    public String toLog() {
        return "";
    }
}
