/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net;

public interface SocketAcceptor {

    void start();

    String getName();

    int getPort();

}
