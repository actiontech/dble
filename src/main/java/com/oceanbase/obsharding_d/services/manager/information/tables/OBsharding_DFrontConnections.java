/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.TimeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OBsharding_DFrontConnections extends ManagerBaseTable {
    public OBsharding_DFrontConnections() {
        super("session_connections", 21);
    }

    @Override
    protected void initColumnAndType() {

        columns.put("session_conn_id", new ColumnMeta("session_conn_id", "int(11)", false, true));
        columnsType.put("session_conn_id", Fields.FIELD_TYPE_LONG);

        columns.put("remote_addr", new ColumnMeta("remote_addr", "varchar(64)", false));
        columnsType.put("remote_addr", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("remote_port", new ColumnMeta("remote_port", "int(11)", false));
        columnsType.put("remote_port", Fields.FIELD_TYPE_LONG);

        columns.put("local_port", new ColumnMeta("local_port", "int(11)", false));
        columnsType.put("local_port", Fields.FIELD_TYPE_LONG);

        columns.put("processor_id", new ColumnMeta("processor_id", "varchar(64)", false));
        columnsType.put("processor_id", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("user", new ColumnMeta("user", "varchar(64)", false));
        columnsType.put("user", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("tenant", new ColumnMeta("tenant", "varchar(64)", false));
        columnsType.put("tenant", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("schema", new ColumnMeta("schema", "varchar(64)", false));
        columnsType.put("schema", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("sql", new ColumnMeta("sql", "varchar(1024)", false));
        columnsType.put("sql", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("sql_execute_time", new ColumnMeta("sql_execute_time", "int(11)", false));
        columnsType.put("sql_execute_time", Fields.FIELD_TYPE_LONG);

        columns.put("sql_start_timestamp", new ColumnMeta("sql_start_timestamp", "int(11)", false));
        columnsType.put("sql_start_timestamp", Fields.FIELD_TYPE_LONG);

        columns.put("sql_stage", new ColumnMeta("sql_stage", "varchar(64)", false));
        columnsType.put("sql_stage", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("conn_net_in", new ColumnMeta("conn_net_in", "int(11)", false));
        columnsType.put("conn_net_in", Fields.FIELD_TYPE_LONG);

        columns.put("conn_net_out", new ColumnMeta("conn_net_out", "int(11)", false));
        columnsType.put("conn_net_out", Fields.FIELD_TYPE_LONG);

        columns.put("conn_estab_time", new ColumnMeta("conn_estab_time", "int(11)", false));
        columnsType.put("conn_estab_time", Fields.FIELD_TYPE_LONG);

        columns.put("conn_recv_buffer", new ColumnMeta("conn_recv_buffer", "int(11)", false));
        columnsType.put("conn_recv_buffer", Fields.FIELD_TYPE_LONG);

        columns.put("conn_send_task_queue", new ColumnMeta("conn_send_task_queue", "int(11)", false));
        columnsType.put("conn_send_task_queue", Fields.FIELD_TYPE_LONG);

        columns.put("conn_recv_task_queue", new ColumnMeta("conn_recv_task_queue", "int(11)", false));
        columnsType.put("conn_recv_task_queue", Fields.FIELD_TYPE_LONG);

        columns.put("in_transaction", new ColumnMeta("in_transaction", "varchar(5)", false));
        columnsType.put("in_transaction", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("xa_id", new ColumnMeta("xa_id", "varchar(5)", false));
        columnsType.put("xa_id", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("entry_id", new ColumnMeta("entry_id", "int(11)", false));
        columnsType.put("entry_id", Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(100);
        Map<UserName, UserConfig> users = OBsharding_DServer.getInstance().getConfig().getUsers();
        for (IOProcessor p : OBsharding_DServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc == null || !fc.isAuthorized()) {
                    continue;
                }
                if (!fc.isClosed()) {
                    lst.add(getRow(fc, users));
                }
            }
        }
        return lst;
    }

    private LinkedHashMap<String, String> getRow(FrontendConnection c, Map<UserName, UserConfig> users) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();

        row.put("session_conn_id", c.getId() + "");
        row.put("remote_addr", c.getHost());
        row.put("remote_port", c.getPort() + "");
        row.put("local_port", c.getLocalPort() + "");
        row.put("processor_id", c.getProcessor().getName());

        FrontendService service = (FrontendService) c.getService();
        row.put("user", service.getUser().getName());
        row.put("tenant", service.getUser().getTenant() == null ? "NULL" : service.getUser().getTenant());


        long rt = c.getLastReadTime();
        long wt = c.getLastWriteTime();
        row.put("sql_execute_time", ((wt >= rt) ? (wt - rt) : (TimeUtil.currentTimeMillis() - rt)) + "");
        row.put("sql_start_timestamp", rt + "");

        if (service.getExecuteSql() != null) {
            row.put("sql", service.getExecuteSql().length() <= 1024 ? service.getExecuteSql() : service.getExecuteSql().substring(0, 1024));
        }

        if (c.isManager()) {
            row.put("sql_stage", "Manager connection");
            row.put("in_transaction", "Manager connection");
            row.put("xa_id", "-");
        } else {
            if (service instanceof ShardingService) {
                NonBlockingSession session = ((ShardingService) service).getSession2();
                row.put("in_transaction", String.valueOf(((ShardingService) service).isInTransaction()));
                row.put("sql_stage", session.getSessionStage().toString());
                String xaid = session.getSessionXaID();
                row.put("xa_id", xaid == null ? "NULL" : xaid);
            } else {
                row.put("in_transaction", !service.isAutocommit() + "");
                row.put("sql_stage", "NULL");
                row.put("xa_id", "-");
            }
        }
        row.put("schema", service.getSchema() == null ? "NULL" : service.getSchema());
        row.put("conn_net_in", c.getNetInBytes() + "");
        row.put("conn_net_out", c.getNetOutBytes() + "");
        row.put("conn_estab_time", ((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000) + "");
        ByteBuffer bb = c.getBottomReadBuffer();
        row.put("conn_recv_buffer", (bb == null ? 0 : bb.capacity()) + "");
        row.put("conn_send_task_queue", c.getWriteQueue().size() + "");
        row.put("conn_recv_task_queue", c.getFrontEndService().getRecvTaskQueueSize() + "");
        row.put("entry_id", users.get(service.getUser()).getId() + "");
        return row;
    }

}
