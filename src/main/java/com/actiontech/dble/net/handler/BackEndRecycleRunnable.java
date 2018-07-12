package com.actiontech.dble.net.handler;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by szf on 2018/7/12.
 */
public class BackEndRecycleRunnable implements Runnable {

    private final MySQLConnection backendConnection;
    private ReentrantLock lock = new ReentrantLock();
    private Condition condRelease = this.lock.newCondition();

    public BackEndRecycleRunnable(MySQLConnection backendConnection) {
        this.backendConnection = backendConnection;
        backendConnection.setRecycler(this);
    }


    @Override
    public void run() {
        try {
            lock.lock();
            if (backendConnection.isRunning()) {

                if (!condRelease.await(10, TimeUnit.MILLISECONDS)) {
                    backendConnection.close("recycle time out");
                } else {
                    backendConnection.release();
                }
            } else {
                backendConnection.release();
            }
        } catch (Exception e) {
            backendConnection.close("recycle exception");
        } finally {
            lock.unlock();
        }
    }


    public void singal() {
        lock.lock();
        try {
            condRelease.signal();
        } finally {
            lock.unlock();
        }
    }

}
