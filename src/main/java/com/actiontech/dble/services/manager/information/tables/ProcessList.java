package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.manager.handler.ShowProcesslistHandler;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.CollectionUtil;
import com.google.common.collect.Maps;

import java.util.*;

public class ProcessList extends ManagerBaseTable {

    private static final String TABLE_NAME = "processlist";

    private static final String COLUMN_FRONT_ID = "front_id";
    private static final String COLUMN_SHARDING_NODE = "sharding_node";
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
        columns.put(COLUMN_FRONT_ID, new ColumnMeta(COLUMN_FRONT_ID, "int(11)", false));
        columnsType.put(COLUMN_FRONT_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SHARDING_NODE, new ColumnMeta(COLUMN_SHARDING_NODE, "varchar(12)", false));
        columnsType.put(COLUMN_SHARDING_NODE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DB_INSTANCE, new ColumnMeta(COLUMN_DB_INSTANCE, "varchar(12)", false));
        columnsType.put(COLUMN_DB_INSTANCE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MYSQL_ID, new ColumnMeta(COLUMN_MYSQL_ID, "int(11)", false));
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
        Map<String, List<Long>> shardingNodeMap = new HashMap<>(4, 1f);

        List<LinkedHashMap<String, String>> rows = new ArrayList<>();

        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            p.getFrontends().
                    values().
                    forEach(fc -> {
                        Map<RouteResultsetNode, BackendConnection> backendConns = null;
                        if (!fc.isManager() && fc.getService() instanceof ShardingService) {
                            backendConns = ((ShardingService) fc.getService()).getSession2().getTargetMap();
                        }

                        if (!CollectionUtil.isEmpty(backendConns)) {
                            for (Map.Entry<RouteResultsetNode, BackendConnection> entry : backendConns.entrySet()) {
                                String shardingNode = entry.getKey().getName();
                                long threadId = entry.getValue().getThreadId();

                                LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
                                // Front_Id
                                row.put(COLUMN_FRONT_ID, fc.getId() + "");
                                // shardingNode
                                row.put(COLUMN_SHARDING_NODE, shardingNode);
                                // dbInstance
                                row.put(COLUMN_DB_INSTANCE, entry.getValue().getInstance().getConfig().getInstanceName());
                                // BconnID
                                row.put(COLUMN_MYSQL_ID, threadId + "");
                                // User
                                row.put(COLUMN_USER, fc.getFrontEndService().getUser().toString());
                                // Front_Host
                                row.put(COLUMN_FRONT_HOST, fc.getHost() + ":" + fc.getLocalPort());
                                // time
                                row.put(COLUMN_TIME, "0");
                                // state
                                row.put(COLUMN_STATE, "");

                                rows.add(row);
                                // index
                                indexs.put(shardingNode + "." + threadId, rows.size() - 1);
                                // sharding node map
                                if (shardingNodeMap.get(shardingNode) == null) {
                                    List<Long> threadIds = new ArrayList<>(3);
                                    threadIds.add(threadId);
                                    shardingNodeMap.put(shardingNode, threadIds);
                                } else {
                                    shardingNodeMap.get(shardingNode).add(threadId);
                                }
                            }
                        } else {
                            LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
                            // Front_Id
                            row.put(COLUMN_FRONT_ID, fc.getId() + "");
                            // User
                            row.put(COLUMN_USER, fc.getFrontEndService().getUser().toString());
                            // Front_Host
                            row.put(COLUMN_FRONT_HOST, fc.getHost() + ":" + fc.getLocalPort());
                            // time
                            row.put(COLUMN_TIME, "0");
                            // state
                            row.put(COLUMN_STATE, "");
                            rows.add(row);
                        }
                    });
        }

        // set 'show processlist' content
        if (!shardingNodeMap.isEmpty()) {
            Map<String, Map<String, String>> backendRes = showProcessList(shardingNodeMap);
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

    private Map<String, Map<String, String>> showProcessList(Map<String, List<Long>> dns) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : dns.entrySet()) {
            ShowProcesslistHandler handler = new ShowProcesslistHandler(entry.getKey(), entry.getValue());
            handler.execute();
            if (handler.getResult() != null) {
                result.putAll(handler.getResult());
            }
        }
        return result;
    }
}
