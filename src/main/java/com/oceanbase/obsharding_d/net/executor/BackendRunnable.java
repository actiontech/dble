/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.executor;


/**
 * Created by szf on 2020/6/18.
 */
public interface BackendRunnable extends Runnable {
    ThreadContextView getThreadContext();
}
