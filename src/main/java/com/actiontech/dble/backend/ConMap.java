/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.util.StringUtil;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConMap {
    // key--schema
    private final ConcurrentMap<String, ConQueue> items = new ConcurrentHashMap<>();

    private static final String KEY_STRING_FOR_NULL_DATABASE = "KEY FOR NULL";

    public ConQueue createAndGetSchemaConQueue(String schema) {
        ConQueue queue = items.get(schema == null ? KEY_STRING_FOR_NULL_DATABASE : schema);
        if (queue == null) {
            ConQueue newQueue = new ConQueue();
            queue = items.putIfAbsent(schema == null ? KEY_STRING_FOR_NULL_DATABASE : schema, newQueue);
            return (queue == null) ? newQueue : queue;
        }
        return queue;
    }

    public ConQueue getSchemaConQueue(String schema) {
        return items.get(schema == null ? KEY_STRING_FOR_NULL_DATABASE : schema);
    }

    public BackendConnection tryTakeCon(final String schema, boolean autoCommit) {

        final ConQueue queue = items.get(schema == null ? KEY_STRING_FOR_NULL_DATABASE : schema);
        BackendConnection con = null;
        if (queue != null) {
            con = tryTakeCon(queue, autoCommit);
        }
        if (con != null) {
            return con;
        } else {
            for (ConQueue queue2 : items.values()) {
                if (queue != queue2) {
                    con = tryTakeCon(queue2, autoCommit);
                    if (con != null) {
                        return con;
                    }
                }
            }
        }
        return null;

    }

    private BackendConnection tryTakeCon(ConQueue queue, boolean autoCommit) {

        BackendConnection con;
        if (queue != null && ((con = queue.takeIdleCon(autoCommit)) != null)) {
            return con;
        } else {
            return null;
        }
    }

    public Collection<ConQueue> getAllConQueue() {
        return items.values();
    }

    public int getActiveCountForSchema(String schema, PhysicalDataSource dataSource) {
        int total = 0;
        for (NIOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
            for (BackendConnection con : processor.getBackends().values()) {
                if (con instanceof MySQLConnection) {
                    MySQLConnection mysqlCon = (MySQLConnection) con;

                    if (StringUtil.equals(mysqlCon.getSchema(), schema) &&
                            mysqlCon.getPool() == dataSource &&
                            mysqlCon.isBorrowed()) {
                        total++;
                    }
                }
            }
        }
        return total;
    }


    public void clearConnections(String reason, PhysicalDataSource dataSource) {
        for (NIOProcessor processor : DbleServer.getInstance().getBackendProcessors()) {
            ConcurrentMap<Long, BackendConnection> map = processor.getBackends();
            Iterator<Entry<Long, BackendConnection>> iterator = map.entrySet().iterator();

            while (iterator.hasNext()) {
                Entry<Long, BackendConnection> entry = iterator.next();
                BackendConnection con = entry.getValue();
                if (con instanceof MySQLConnection) {
                    if (((MySQLConnection) con).getPool() == dataSource) {
                        con.close(reason);
                        iterator.remove();
                    }
                }
            }
        }
        items.clear();
    }
}
