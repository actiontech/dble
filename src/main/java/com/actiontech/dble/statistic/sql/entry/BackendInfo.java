package com.actiontech.dble.statistic.sql.entry;

import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.net.connection.BackendConnection;

public class BackendInfo {
    String name; // db_instance
    String host;
    int port;

    public BackendInfo(BackendConnection bConn) {
        this.name = ((MySQLInstance) bConn.getInstance()).getName();
        this.host = bConn.getHost();
        this.port = bConn.getPort();
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
