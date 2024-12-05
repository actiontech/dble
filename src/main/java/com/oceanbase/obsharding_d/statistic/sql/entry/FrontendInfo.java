/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.entry;

public class FrontendInfo {
    int userId;
    String user;
    String host;
    int port;

    public FrontendInfo(int userId, String user, String host, int port) {
        this.userId = userId;
        this.user = user;
        this.host = host;
        this.port = port;
    }

    public int getUserId() {
        return userId;
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
