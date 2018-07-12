package com.actiontech.dble.net.handler;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2018/7/12.
 */
public class BackEndRecycleRunnable implements Runnable {

    private final MySQLConnection backendConnection;

    public BackEndRecycleRunnable(MySQLConnection backendConnection) {
        this.backendConnection = backendConnection;
    }


    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 1000) {
            if (backendConnection.isRunning()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                break;
            } else {
                backendConnection.release();
                return;
            }
        }
        backendConnection.close("recycle time out");
    }

}
