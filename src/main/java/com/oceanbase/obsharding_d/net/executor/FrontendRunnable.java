/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.executor;


/**
 * Created by szf on 2020/6/18.
 */
public interface FrontendRunnable extends Runnable {
    ThreadContextView getThreadContext();
}
