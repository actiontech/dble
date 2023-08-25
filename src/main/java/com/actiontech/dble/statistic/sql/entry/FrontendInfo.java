/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.entry;

import com.actiontech.dble.services.FrontendService;

public class FrontendInfo {
    int userId;
    String user;
    String host;
    int port;

    public FrontendInfo(FrontendService service) {
        this.userId = service.getUserConfig().getId();
        this.user = service.getUser().getFullName();
        this.host = service.getConnection().getHost();
        this.port = service.getConnection().getPort();
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
