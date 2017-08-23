package io.mycat.plan.common.time;

public class Interval {
    public long year, month, day, hour;
    public long minute, second, secondPart;
    public boolean neg;

    public Interval() {
        year = month = day = hour = minute = second = secondPart = 0;
        neg = false;
    }
}
