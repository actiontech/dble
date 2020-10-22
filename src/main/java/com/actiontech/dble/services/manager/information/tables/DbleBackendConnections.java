/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.heartbeat.HeartbeatSQLJob;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public final class DbleBackendConnections extends ManagerBaseTable {
    public DbleBackendConnections() {
        super("backend_connections", 24);
    }

    @Override
    protected void initColumnAndType() {

        columns.put("backend_conn_id", new ColumnMeta("backend_conn_id", "int(11)", false, true));
        columnsType.put("backend_conn_id", Fields.FIELD_TYPE_LONG);

        columns.put("db_group_name", new ColumnMeta("db_group_name", "varchar(64)", false));
        columnsType.put("db_group_name", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("db_instance_name", new ColumnMeta("db_instance_name", "varchar(64)", false));
        columnsType.put("db_instance_name", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("remote_addr", new ColumnMeta("remote_addr", "varchar(16)", false));
        columnsType.put("remote_addr", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("remote_port", new ColumnMeta("remote_port", "int(11)", false));
        columnsType.put("remote_port", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("remote_processlist_id", new ColumnMeta("remote_processlist_id", "int(11)", false));
        columnsType.put("remote_processlist_id", Fields.FIELD_TYPE_LONG);

        columns.put("local_port", new ColumnMeta("local_port", "int(11)", false));
        columnsType.put("local_port", Fields.FIELD_TYPE_LONG);

        columns.put("processor_id", new ColumnMeta("processor_id", "varchar(16)", false));
        columnsType.put("processor_id", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("user", new ColumnMeta("user", "varchar(64)", false));
        columnsType.put("user", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("schema", new ColumnMeta("schema", "varchar(16)", false));
        columnsType.put("schema", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("session_conn_id", new ColumnMeta("session_conn_id", "int(11)", false));
        columnsType.put("session_conn_id", Fields.FIELD_TYPE_LONG);

        columns.put("sql", new ColumnMeta("sql", "varchar(1024)", false));
        columnsType.put("sql", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("sql_execute_time", new ColumnMeta("sql_execute_time", "int(11)", false));
        columnsType.put("sql_execute_time", Fields.FIELD_TYPE_LONG);

        columns.put("mark_as_expired_timestamp", new ColumnMeta("mark_as_expired_timestamp", "int(11)", false));
        columnsType.put("mark_as_expired_timestamp", Fields.FIELD_TYPE_LONG);

        columns.put("conn_net_in", new ColumnMeta("conn_net_in", "int(11)", false));
        columnsType.put("conn_net_in", Fields.FIELD_TYPE_LONG);

        columns.put("conn_net_out", new ColumnMeta("conn_net_out", "int(11)", false));
        columnsType.put("conn_net_out", Fields.FIELD_TYPE_LONG);

        columns.put("conn_estab_time", new ColumnMeta("conn_estab_time", "int(11)", false));
        columnsType.put("conn_estab_time", Fields.FIELD_TYPE_LONG);

        columns.put("borrowed_from_pool", new ColumnMeta("borrowed_from_pool", "varchar(5)", false));
        columnsType.put("borrowed_from_pool", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("conn_recv_buffer", new ColumnMeta("conn_recv_buffer", "int(11)", false));
        columnsType.put("conn_recv_buffer", Fields.FIELD_TYPE_LONG);

        columns.put("conn_send_task_queue", new ColumnMeta("conn_send_task_queue", "int(11)", false));
        columnsType.put("conn_send_task_queue", Fields.FIELD_TYPE_LONG);

        columns.put("used_for_heartbeat", new ColumnMeta("used_for_heartbeat", "varchar(5)", false));
        columnsType.put("used_for_heartbeat", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("conn_closing", new ColumnMeta("conn_closing", "varchar(5)", false));
        columnsType.put("conn_closing", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("xa_status", new ColumnMeta("xa_status", "varchar(64)", false));
        columnsType.put("xa_status", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("in_transaction", new ColumnMeta("in_transaction", "varchar(5)", false));
        columnsType.put("in_transaction", Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(100);
        for (IOProcessor p : DbleServer.getInstance().getBackendProcessors()) {
            for (BackendConnection bc : p.getBackends().values()) {
                lst.add(getRow(bc));
            }
        }
        return lst;
    }

    private LinkedHashMap<String, String> getRow(BackendConnection c) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();

        row.put("backend_conn_id", c.getId() + "");
        ReadTimeStatusInstance instance = c.getInstance();
        if (instance != null) {
            row.put("db_group_name", instance.getDbGroupConfig().getName());
            row.put("db_instance_name", instance.getConfig().getInstanceName());
            row.put("user", instance.getConfig().getUser());
        }
        row.put("remote_addr", c.getHost());
        row.put("remote_port", c.getPort() + "");
        row.put("remote_processlist_id", c.getThreadId() + "");
        row.put("local_port", c.getLocalPort() + "");
        row.put("processor_id", c.getProcessor().getName());
        row.put("schema", c.getSchema() == null ? "NULL" : c.getSchema());

        MySQLResponseService service = c.getBackendService();
        if (service.getSession() != null) {
            row.put("session_conn_id", service.getSession().getSource().getId() + "");
        }
        row.put("conn_estab_time", ((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000) + "");
        ByteBuffer bb = c.getReadBuffer();
        row.put("conn_recv_buffer", (bb == null ? 0 : bb.capacity()) + "");
        row.put("conn_send_task_queue", c.getWriteQueue().size() + "");

        RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
        if (rrn != null) {
            String sql = rrn.getStatement();
            if (sql.length() > 1024) {
                row.put("sql", sql.substring(0, 1024).replaceAll("[\n\t]", " "));
            } else {
                row.put("sql", sql.replaceAll("[\n\t]", " "));
            }
        }
        long rt = c.getLastReadTime();
        long wt = c.getLastWriteTime();
        row.put("sql_execute_time", ((wt >= rt) ? (wt - rt) : (TimeUtil.currentTimeMillis() - rt)) + "");
        row.put("mark_as_expired_timestamp", c.getPoolDestroyedTime() + "");
        row.put("conn_net_in", c.getNetInBytes() + "");
        row.put("conn_net_out", c.getNetOutBytes() + "");

        if (c.getState() == PooledConnection.INITIAL) {
            ResponseHandler handler = service.getResponseHandler();
            row.put("used_for_heartbeat", handler instanceof HeartbeatSQLJob ? "true" : "false");
            row.put("borrowed_from_pool", "true");
        } else {
            row.put("used_for_heartbeat", "false");
            row.put("borrowed_from_pool", "true");
        }

        row.put("conn_closing", c.isClosed() ? "true" : "false");
        if (service.getXaStatus() != null) {
            row.put("xa_status", service.getXaStatus().toString());
        }
        row.put("in_transaction", !service.isAutocommit() + "");
        return row;
    }

}
