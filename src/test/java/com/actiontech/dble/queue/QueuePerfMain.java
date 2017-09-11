/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.queue;

import jsr166y.LinkedTransferQueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Queue performance test
 *
 * @author mycat
 */
public class QueuePerfMain {

    private static byte[] testData = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

    private static BlockingQueue<byte[]> arrayQueue = new ArrayBlockingQueue<byte[]>(5000000);
    private static FixedQueue<byte[]> fixedQueue = new FixedQueue<byte[]>(5000000);
    private static Queue<byte[]> testQueue = new Queue<byte[]>();
    private static BlockingQueue<byte[]> linkedQueue = new LinkedBlockingQueue<byte[]>();
    private static LinkedTransferQueue<byte[]> transferQueue = new LinkedTransferQueue<byte[]>();

    public static void tArrayQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    arrayQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += arrayQueue.size();
                    arrayQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tFixedQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    fixedQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += fixedQueue.size();
                    fixedQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    testQueue.append(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += testQueue.size();
                    testQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tLinkedQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    linkedQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += linkedQueue.size();
                    linkedQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void tTransferQueue() {
        new Thread() {

            @Override
            public void run() {
                while (true) {
                    transferQueue.offer(testData);
                }
            }
        }.start();

        new Thread() {

            @Override
            public void run() {
                int count = 0;
                long num = 0;
                while (true) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                    count++;
                    num += transferQueue.size();
                    transferQueue.clear();
                    if (count == 50) {
                        System.out.println(num / 50);
                        count = 0;
                        num = 0;
                    }
                }
            }
        }.start();
    }

    public static void main(String[] args) {
        // testArrayQueue();
        // testFixedQueue();
        // testQueue();
        // testLinkedQueue();
        // testTransferQueue();
    }

}