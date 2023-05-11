package com.actiontech.dble.statistic.sql.entry;

import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.net.connection.BackendConnection;

public class BackendInfo {
    String name; // db_instance
    String host;
    int port;
    String node; // sharding_node

    public BackendInfo(BackendConnection bConn, String node) {
        this.name = ((MySQLInstance) bConn.getInstance()).getName();
        this.host = bConn.getHost();
        this.port = bConn.getPort();
        this.node = node;
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

    public String getNode() {
        return node;
    }
}
