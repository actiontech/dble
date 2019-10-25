package com.actiontech.dble.plan;

/**
 * Created by szf on 2019/10/25.
 */
public class OpCounter {

    private static final OpCounter OP = new OpCounter();

    private volatile int count = 0;

    public void add() {
        count++;
    }

    public static OpCounter getInstance() {
        return OP;
    }
}
