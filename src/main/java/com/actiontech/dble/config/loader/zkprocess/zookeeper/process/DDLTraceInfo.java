package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.route.RouteResultsetNode;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2019/11/29.
 */
public class DDLTraceInfo {


    public enum DDLStage {
        ROUTE_END, LOCK_END, LINK_TEST_START, LINK_TEST_END, EXECUTE_START, EXECUTE_END, META_UPDATE
    }

    public enum DDLConnectionStatus {
        TEST_START, TEST_SUCCESS, TEST_CONN_ERROR, TEST_ERROR, TEST_CONN_CLOSE, EXECUTE_START, EXECUTE_SUCCESS, EXECUTE_CONN_ERROR, EXECUTE_ERROR, EXECUTE_CONN_CLOSE
    }

    private final long serverId;
    private final int id;
    private final String sql;
    private final long startTimestamp;
    private final Map<MySQLConnection, DDLConnectionStatus> testConnections = new ConcurrentHashMap();
    private final Map<MySQLConnection, DDLConnectionStatus> executeConnections = new ConcurrentHashMap();
    private volatile DDLStage stage;

    public DDLTraceInfo(long serverId, String sql, int id) {
        this.serverId = serverId;
        this.sql = sql;
        startTimestamp = System.currentTimeMillis();
        //ddl trace only start form route-finished DDL SQL,
        // because DDL which not pass the route has no risk of inconsistency
        stage = DDLStage.ROUTE_END;
        this.id = id;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("[DDL][");
        sb.append(id).append("]SQL = ").append(sql);
        sb.append(" serverConnection id = ").append(serverId);
        sb.append(" startTime = ").append(new Date(startTimestamp));
        sb.append(" stage = ").append(stage);
        if (testConnections.size() > 0) {
            sb.append("\n test connections \n:");
            for (Map.Entry<MySQLConnection, DDLConnectionStatus> entry : testConnections.entrySet()) {
                sb.append(" -----------------connection:");
                sb.append(entry.getKey().getId()).append(" status: ").append(entry.getValue());
                sb.append(" dataNode: ").append(((RouteResultsetNode) entry.getKey().getAttachment()).getName()).append("\n");

            }
        }
        if (executeConnections.size() > 0) {
            sb.append("\n execute connections \n:");
            for (Map.Entry<MySQLConnection, DDLConnectionStatus> entry : executeConnections.entrySet()) {
                sb.append(" -----------------connection:'\n");
                sb.append("                              ").append(entry.getKey()).append(" \n                          status: ").
                        append(entry.getValue()).append("\n");
            }
        }
        return sb.toString();
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStage(DDLStage stage) {
        this.stage = stage;
    }

    public int getId() {
        return id;
    }

    public void updateConnectionStatus(MySQLConnection c, DDLConnectionStatus status) {
        switch (status) {
            case TEST_START:
            case TEST_SUCCESS:
            case TEST_ERROR:
            case TEST_CONN_ERROR:
            case TEST_CONN_CLOSE:
                testConnections.put(c, status);
                break;
            case EXECUTE_START:
            case EXECUTE_SUCCESS:
            case EXECUTE_ERROR:
            case EXECUTE_CONN_ERROR:
            case EXECUTE_CONN_CLOSE:
                executeConnections.put(c, status);
                break;
            default:
                //do nothing
        }
    }

}
