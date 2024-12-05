/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

import java.io.IOException;

public interface AuthService {

    void register() throws IOException;

    default void onConnectFailed(Throwable e) {
    }


    boolean haveNotReceivedMessage();

}
