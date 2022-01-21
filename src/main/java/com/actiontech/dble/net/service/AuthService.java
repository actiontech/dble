/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.service;

import java.io.IOException;

public interface AuthService {

    void register() throws IOException;

    default void onConnectFailed(Throwable e) {
    }

}
