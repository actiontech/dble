package com.actiontech.dble.net.executor;


/**
 * Created by szf on 2020/6/18.
 */
public interface FrontendRunnable extends Runnable {
    ThreadContextView getThreadContext();
}
