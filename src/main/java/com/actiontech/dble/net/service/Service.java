/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.service;

import com.actiontech.dble.net.executor.ThreadContext;

/**
 * Created by szf on 2020/6/15.
 */
public interface Service {

    void handle(ServiceTask task);

    void execute(ServiceTask task, ThreadContext threadContext);

    void cleanup();
}
