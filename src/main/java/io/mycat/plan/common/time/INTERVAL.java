package io.mycat.plan.common.time;

public class INTERVAL {
    public long year, month, day, hour;
    public long minute, second, secondPart;
    public boolean neg;

    public INTERVAL() {
        year = month = day = hour = minute = second = secondPart = 0;
        neg = false;
    }
}
