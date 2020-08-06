/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;


import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by szf on 2019/11/29.
 */
public class DDLTraceInfo {


    public enum DDLStage {
        ROUTE_END, LOCK_END, CONN_TEST_START, CONN_TEST_END, EXECUTE_START, EXECUTE_END, META_UPDATE
    }

    public enum DDLConnectionStatus {
        CONN_TEST_START, CONN_TEST_SUCCESS, TEST_CONN_ERROR, CONN_TEST_RESULT_ERROR, TEST_CONN_CLOSE, CONN_EXECUTE_START, CONN_EXECUTE_SUCCESS, EXECUTE_CONN_ERROR, CONN_EXECUTE_ERROR, EXECUTE_CONN_CLOSE
    }

    private final long serverId;
    private final int id;
    private final String sql;
    private final long startTimestamp;
    private final Map<MySQLResponseService, DDLConnectionStatus> testConnections = new ConcurrentHashMap();
    private final Map<MySQLResponseService, DDLConnectionStatus> executeConnections = new ConcurrentHashMap();
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

    public String toBriefString() {
        StringBuilder sb = new StringBuilder("[DDL][");
        sb.append(id).append("]SQL = ").append(sql);
        sb.append(" serverConnection id = ").append(serverId);
        sb.append(" startTime = ").append(new Date(startTimestamp));
        sb.append(" stage = ").append(stage);
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("[DDL][");
        sb.append(id).append("]SQL = ").append(sql);
        sb.append(" serverConnection id = ").append(serverId);
        sb.append(" startTime = ").append(new Date(startTimestamp));
        sb.append(" stage = ").append(stage);
        if (testConnections.size() > 0) {
            sb.append("\n[DDL][").append(id).append("] test connections :");
            for (Map.Entry<MySQLResponseService, DDLConnectionStatus> entry : testConnections.entrySet()) {
                sb.append("\n[DDL][").append(id).append("] connection:");
                sb.append(entry.getKey().getConnection().getId()).append(" status: ").append(entry.getValue());
                sb.append(" shardingNode: ").append(((RouteResultsetNode) entry.getKey().getAttachment()).getName());

            }
        }
        if (executeConnections.size() > 0) {
            sb.append("\n[DDL][").append(id).append("] execute connections :");
            for (Map.Entry<MySQLResponseService, DDLConnectionStatus> entry : executeConnections.entrySet()) {
                sb.append("\n[DDL][").append(id).append("] connection:");
                sb.append(entry.getKey().getConnection().getId()).append(" status: ").append(entry.getValue());
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

    public void updateConnectionStatus(MySQLResponseService responseService, DDLConnectionStatus status) {
        switch (status) {
            case CONN_TEST_START:
            case CONN_TEST_SUCCESS:
            case CONN_TEST_RESULT_ERROR:
            case TEST_CONN_ERROR:
            case TEST_CONN_CLOSE:
                testConnections.put(responseService, status);
                break;
            case CONN_EXECUTE_START:
            case CONN_EXECUTE_SUCCESS:
            case CONN_EXECUTE_ERROR:
            case EXECUTE_CONN_ERROR:
            case EXECUTE_CONN_CLOSE:
                executeConnections.put(responseService, status);
                break;
            default:
                //do nothing
        }
    }

}
