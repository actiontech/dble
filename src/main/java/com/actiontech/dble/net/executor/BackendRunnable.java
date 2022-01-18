/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.executor;


/**
 * Created by szf on 2020/6/18.
 */
public interface BackendRunnable extends Runnable {
    ThreadContextView getThreadContext();
}
