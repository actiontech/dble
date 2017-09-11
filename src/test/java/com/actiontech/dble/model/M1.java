/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.model;

import jsr166y.LinkedTransferQueue;

import java.util.concurrent.BlockingQueue;

/**
 * @author mycat
 */
public class M1 {

    private long count;
    private final BlockingQueue<TransferObject> x;
    private final BlockingQueue<TransferObject> y;

    public M1() {
        this.x = new LinkedTransferQueue<TransferObject>();
        this.y = new LinkedTransferQueue<TransferObject>();
    }

    public long getCount() {
        return count;
    }

    public BlockingQueue<TransferObject> getX() {
        return x;
    }

    public BlockingQueue<TransferObject> getY() {
        return y;
    }

    public void start() {
        new Thread(new A(), "A").start();
        new Thread(new B(), "B").start();
        new Thread(new C(), "C").start();
    }

    private final class A implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                }
                for (int i = 0; i < 1000000; i++) {
                    x.offer(new TransferObject());
                }
            }
        }
    }

    private final class B implements Runnable {
        @Override
        public void run() {
            TransferObject t = null;
            for (; ; ) {
                try {
                    t = x.take();
                } catch (InterruptedException e) {
                    continue;
                }
                t.handle();
                y.offer(t);
            }
        }
    }

    private final class C implements Runnable {
        @Override
        public void run() {
            TransferObject t = null;
            for (; ; ) {
                try {
                    t = y.take();
                } catch (InterruptedException e) {
                    continue;
                }
                t.compelete();
                count++;
            }
        }
    }

}