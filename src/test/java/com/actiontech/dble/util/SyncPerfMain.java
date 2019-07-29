/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class SyncPerfMain {

    long i = 0L;

    private final Object lockA = new Object();
    private final ReentrantLock lockB = new ReentrantLock();

    final void tLockA() {
        final Object lock = this.lockA;
        synchronized (lock) {
            i++;
        }
    }

    final void tLockB() {
        final ReentrantLock lock = this.lockB;
        lock.lock();
        try {
            i++;
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        int count = 10000000;
        SyncPerfMain test = new SyncPerfMain();

        System.currentTimeMillis();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            test.tLockA();
            // test.testLockB();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("take:" + (t2 - t1) + " ms.");
    }

}