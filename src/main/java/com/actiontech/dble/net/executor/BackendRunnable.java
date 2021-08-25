package com.actiontech.dble.net.executor;


/**
 * Created by szf on 2020/6/18.
 */
public interface BackendRunnable extends Runnable {
    ThreadContextView getThreadContext();
}
