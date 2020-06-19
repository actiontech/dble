/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net;

import com.actiontech.dble.net.mysql.CharsetNames;

public interface ClosableConnection {
    CharsetNames getCharset();

    /**
     * close connection
     */
    void close(String reason);

    boolean isClosed();

    long getStartupTime();

    String getHost();

    int getPort();

    int getLocalPort();

    long getNetInBytes();

    long getNetOutBytes();
}
