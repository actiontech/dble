/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

import com.oceanbase.obsharding_d.net.executor.ThreadContext;

/**
 * Created by szf on 2020/6/15.
 */
public interface Service {

    void handle(ServiceTask task);

    void execute(ServiceTask task, ThreadContext threadContext);

    void cleanup();
}
