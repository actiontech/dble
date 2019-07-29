/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class LockPerfMain {

    public void tReentrantLock() {
        System.currentTimeMillis();
        ReentrantLock lock = new ReentrantLock();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            if (lock.tryLock()) {
                try {
                    // ...
                } finally {
                    lock.unlock();
                }
            }
        }
        long t2 = System.currentTimeMillis();

        System.out.println("take time:" + (t2 - t1) + " ms.");
    }

    public void tAtomicBoolean() {
        System.currentTimeMillis();
        AtomicBoolean atomic = new AtomicBoolean();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            if (atomic.compareAndSet(false, true)) {
                try {
                    // ...
                } finally {
                    atomic.set(false);
                }
            }
        }
        long t2 = System.currentTimeMillis();

        System.out.println("take time:" + (t2 - t1) + " ms.");
    }

}