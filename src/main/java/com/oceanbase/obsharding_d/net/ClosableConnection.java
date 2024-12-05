/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net;

import com.oceanbase.obsharding_d.net.mysql.CharsetNames;

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
