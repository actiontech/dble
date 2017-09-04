package com.actiontech.dble.plan.common.time;

public class MySQLTimeStatus {
    private int warnings;
    private long fractionalDigits;
    private long nanoseconds;

    public int getWarnings() {
        return warnings;
    }

    public void setWarnings(int warnings) {
        this.warnings = warnings;
    }

    public long getFractionalDigits() {
        return fractionalDigits;
    }

    public void setFractionalDigits(long fractionalDigits) {
        this.fractionalDigits = fractionalDigits;
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    public void setNanoseconds(long nanoseconds) {
        this.nanoseconds = nanoseconds;
    }
}
