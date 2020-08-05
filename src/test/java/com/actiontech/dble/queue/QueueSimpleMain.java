/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.queue;

import jsr166y.LinkedTransferQueue;

/**
 * @author mycat
 */
public class QueueSimpleMain {

    static long putCount = 0;
    static long takeCount = 0;

    public static void main(String[] args) {
        // final SynchronousQueue<String> queue = new
        // SynchronousQueue<String>();
        // final ArrayBlockingQueue<String> queue = new
        // ArrayBlockingQueue<String>(10000000);
        final LinkedTransferQueue<String> queue = new LinkedTransferQueue<String>();
        // final LinkedBlockingQueue<String> queue = new
        // LinkedBlockingQueue<String>();

        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    long put = putCount;
                    long take = takeCount;
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("put:" + (putCount - put) / 5 + " take:" + (takeCount - take) / 5);
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    if (queue.offer("A")) {
                        putCount++;
                    }
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (; ; ) {
                    // try {
                    if (queue.poll() != null) {
                        takeCount++;
                    }
                    // } catch (InterruptedException e) {
                    // e.printStackTrace();
                    // }
                    // try {
                    // Thread.sleep(10L);
                    // } catch (InterruptedException e) {
                    // 
                    // e.printStackTrace();
                    // }
                }
            }
        }.start();
    }

}