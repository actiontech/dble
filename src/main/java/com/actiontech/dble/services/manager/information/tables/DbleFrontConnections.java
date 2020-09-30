/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.services.FrontEndService;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DbleFrontConnections extends ManagerBaseTable {
    public DbleFrontConnections() {
        super("session_connections", 19);
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

        columns.put("in_transaction", new ColumnMeta("in_transaction", "varchar(5)", false));
        columnsType.put("in_transaction", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("entry_id", new ColumnMeta("entry_id", "int(11)", false));
        columnsType.put("entry_id", Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(100);
        Map<UserName, UserConfig> users = DbleServer.getInstance().getConfig().getUsers();
        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
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

        FrontEndService service = (FrontEndService) (c.getService());
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
            row.put("schema", ((ManagerService) service).getSchema() == null ? "NULL" : ((ManagerService) service).getSchema());
        } else {
            row.put("sql_stage", ((ShardingService) service).getSession2().getSessionStage().toString());
            row.put("in_transaction", !service.isAutocommit() + "");
            row.put("schema", ((ShardingService) service).getSchema() == null ? "NULL" : ((ShardingService) service).getSchema());
        }
        row.put("conn_net_in", c.getNetInBytes() + "");
        row.put("conn_net_out", c.getNetOutBytes() + "");
        row.put("conn_estab_time", ((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000) + "");
        ByteBuffer bb = c.getReadBuffer();
        row.put("conn_recv_buffer", (bb == null ? 0 : bb.capacity()) + "");
        row.put("conn_send_task_queue", c.getWriteQueue().size() + "");
        row.put("entry_id", users.get(service.getUser()).getId() + "");
        return row;
    }

}
