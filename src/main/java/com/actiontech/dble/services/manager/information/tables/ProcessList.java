/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.manager.handler.ShowProcesslistHandler;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.util.CollectionUtil;
import com.google.common.collect.Maps;

import java.util.*;

public class ProcessList extends ManagerBaseTable {

    private static final String TABLE_NAME = "processlist";

    private static final String COLUMN_FRONT_ID = "front_id";
    private static final String COLUMN_DB_INSTANCE = "db_instance";
    private static final String COLUMN_MYSQL_ID = "mysql_id";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_FRONT_HOST = "front_host";
    private static final String COLUMN_MYSQL_DB = "mysql_db";
    private static final String COLUMN_COMMAND = "command";
    private static final String COLUMN_TIME = "time";
    private static final String COLUMN_STATE = "state";
    private static final String COLUMN_INFO = "info";

    public ProcessList() {
        super(TABLE_NAME, 11);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_FRONT_ID, new ColumnMeta(COLUMN_FRONT_ID, "int(11)", false, true));
        columnsType.put(COLUMN_FRONT_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DB_INSTANCE, new ColumnMeta(COLUMN_DB_INSTANCE, "varchar(12)", false));
        columnsType.put(COLUMN_DB_INSTANCE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MYSQL_ID, new ColumnMeta(COLUMN_MYSQL_ID, "int(11)", false, true));
        columnsType.put(COLUMN_MYSQL_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(12)", false));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_FRONT_HOST, new ColumnMeta(COLUMN_FRONT_HOST, "varchar(16)", false));
        columnsType.put(COLUMN_FRONT_HOST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MYSQL_DB, new ColumnMeta(COLUMN_MYSQL_DB, "varchar(16)", false));
        columnsType.put(COLUMN_MYSQL_DB, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_COMMAND, new ColumnMeta(COLUMN_COMMAND, "varchar(1024)", true));
        columnsType.put(COLUMN_COMMAND, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TIME, new ColumnMeta(COLUMN_TIME, "int(11)", true));
        columnsType.put(COLUMN_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_STATE, new ColumnMeta(COLUMN_STATE, "varchar(64)", true));
        columnsType.put(COLUMN_STATE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_INFO, new ColumnMeta(COLUMN_INFO, "varchar(64)", true));
        columnsType.put(COLUMN_INFO, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {

        Map<String, Integer> indexs = new HashMap<>();
        List<LinkedHashMap<String, String>> rows = new ArrayList<>();
        Map<PhysicalDbInstance, List<Long>> dbInstanceMap = new HashMap<>(8);

        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            p.getFrontends().
                    values().
                    forEach(fc -> {
                        if (!fc.isAuthorized() || fc.isManager()) {
                            return;
                        }
                        if (fc.getService() instanceof ShardingService) {
                            Map<RouteResultsetNode, BackendConnection> backendConns = ((ShardingService) fc.getService()).getSession2().getTargetMap();
                            if (!CollectionUtil.isEmpty(backendConns)) {
                                for (Map.Entry<RouteResultsetNode, BackendConnection> entry : backendConns.entrySet()) {
                                    addRow(fc, entry.getValue(), rows, indexs, dbInstanceMap);
                                }
                            } else {
                                rows.add(getDefaultRow(fc));
                            }
                        } else {
                            BackendConnection conn = ((RWSplitService) fc.getService()).getSession2().getConn();
                            if (conn != null) {
                                addRow(fc, conn, rows, indexs, dbInstanceMap);
                            } else {
                                rows.add(getDefaultRow(fc));
                            }
                        }
                    });
        }

        // set 'show processlist' content
        if (!dbInstanceMap.isEmpty()) {
            Map<String, Map<String, String>> backendRes = showProcessList(dbInstanceMap);
            for (Map.Entry<String, Integer> entry : indexs.entrySet()) {
                Map<String, String> res = backendRes.get(entry.getKey());
                if (res != null) {
                    int index = entry.getValue();
                    rows.get(index).put(COLUMN_MYSQL_DB, res.get("db"));
                    rows.get(index).put(COLUMN_COMMAND, res.get("Command"));
                    rows.get(index).put(COLUMN_TIME, res.get("Time"));
                    rows.get(index).put(COLUMN_STATE, res.get("State"));
                    rows.get(index).put(COLUMN_INFO, res.get("info"));
                }
            }
        }

        return rows;
    }

    private void addRow(FrontendConnection fc, BackendConnection bconn, List<LinkedHashMap<String, String>> rows,
                        Map<String, Integer> indexs, Map<PhysicalDbInstance, List<Long>> dbInstanceMap) {
        long threadId = bconn.getThreadId();
        PhysicalDbInstance dbInstance = (PhysicalDbInstance) bconn.getInstance();
        rows.add(getRow(fc, bconn));
        // index
        indexs.put(dbInstance.getName() + "." + threadId, rows.size() - 1);
        // dbInstance map
        if (dbInstanceMap.get(dbInstance) == null) {
            List<Long> threadIds = new ArrayList<>(10);
            threadIds.add(threadId);
            dbInstanceMap.put(dbInstance, threadIds);
        } else {
            dbInstanceMap.get(dbInstance).add(threadId);
        }
    }

    private LinkedHashMap<String, String> getRow(FrontendConnection frontConn, BackendConnection conn) {
        LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
        // Front_Id
        row.put(COLUMN_FRONT_ID, frontConn.getId() + "");
        // dbInstance
        row.put(COLUMN_DB_INSTANCE, conn.getInstance().getConfig().getInstanceName());
        // BconnID
        row.put(COLUMN_MYSQL_ID, conn.getThreadId() + "");
        // User
        row.put(COLUMN_USER, frontConn.getFrontEndService().getUser().getFullName());
        // Front_Host
        row.put(COLUMN_FRONT_HOST, frontConn.getHost() + ":" + frontConn.getLocalPort());
        // time
        row.put(COLUMN_TIME, "0");
        // state
        row.put(COLUMN_STATE, "");
        return row;
    }

    private LinkedHashMap<String, String> getDefaultRow(FrontendConnection frontConn) {
        LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
        // Front_Id
        row.put(COLUMN_FRONT_ID, frontConn.getId() + "");
        // User
        row.put(COLUMN_USER, frontConn.getFrontEndService().getUser().getFullName());
        // Front_Host
        row.put(COLUMN_FRONT_HOST, frontConn.getHost() + ":" + frontConn.getLocalPort());
        // time
        row.put(COLUMN_TIME, "0");
        // state
        row.put(COLUMN_STATE, "");
        return row;
    }

    private Map<String, Map<String, String>> showProcessList(Map<PhysicalDbInstance, List<Long>> dns) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<PhysicalDbInstance, List<Long>> entry : dns.entrySet()) {
            ShowProcesslistHandler handler = new ShowProcesslistHandler(entry.getKey(), entry.getValue());
            handler.execute();
            if (handler.getResult() != null) {
                result.putAll(handler.getResult());
            }
        }
        return result;
    }
}
