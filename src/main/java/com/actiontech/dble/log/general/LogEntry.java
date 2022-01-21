/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.general;

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
