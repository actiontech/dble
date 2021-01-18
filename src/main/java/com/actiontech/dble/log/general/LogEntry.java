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
