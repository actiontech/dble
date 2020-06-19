package com.actiontech.dble.backend.pool;

public interface PooledEntry {

    int STATE_REMOVED = -4;
    int STATE_HEARTBEAT = -3;
    int STATE_RESERVED = -2;
    int STATE_IN_USE = -1;
    int INITIAL = 0;
    int STATE_NOT_IN_USE = 1;

    boolean compareAndSet(int expect, int update);

    void lazySet(int update);

    int getState();

    void release();

}
