/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net;

public interface SocketConnector {

    void start();

    void postConnect(com.oceanbase.obsharding_d.net.connection.AbstractConnection c);
}
