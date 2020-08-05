/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;

import com.actiontech.dble.util.ExecutorUtil;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public class ExecutorTestMain {

    public static void main(String[] args) {
        final AtomicLong count = new AtomicLong(0L);
        final ThreadPoolExecutor executor = ExecutorUtil.createFixed("TestExecutor", 5);

        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    long c = count.get();
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("count:" + (count.get() - c) / 5);
                    System.out.println("active:" + executor.getActiveCount());
                    System.out.println("queue:" + executor.getQueue().size());
                    System.out.println("============================");
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            count.incrementAndGet();
                        }
                    });
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            count.incrementAndGet();
                        }
                    });
                }
            }
        }.start();
    }

}