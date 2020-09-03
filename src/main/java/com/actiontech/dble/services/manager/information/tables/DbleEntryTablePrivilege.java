package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.*;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.sqlengine.MultiRowSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DbleEntryTablePrivilege extends ManagerBaseTable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DbleEntryTablePrivilege.class);

    private static final String TABLE_NAME = "dble_entry_table_privilege";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_SCHEMA = "schema";
    private static final String COLUMN_TABLE = "table";
    private static final String COLUMN_INSERT = "insert";
    private static final String COLUMN_UPDATE = "update";
    private static final String COLUMN_SELECT = "select";
    private static final String COLUMN_DELETE = "delete";

    public DbleEntryTablePrivilege() {
        super(TABLE_NAME, 7);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "int(11)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SCHEMA, new ColumnMeta(COLUMN_SCHEMA, "varchar(64)", false, true));
        columnsType.put(COLUMN_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TABLE, new ColumnMeta(COLUMN_TABLE, "varchar(64)", false, true));
        columnsType.put(COLUMN_TABLE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_INSERT, new ColumnMeta(COLUMN_INSERT, "int(1)", false));
        columnsType.put(COLUMN_INSERT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_UPDATE, new ColumnMeta(COLUMN_UPDATE, "int(1)", false));
        columnsType.put(COLUMN_UPDATE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SELECT, new ColumnMeta(COLUMN_SELECT, "int(1)", false));
        columnsType.put(COLUMN_SELECT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DELETE, new ColumnMeta(COLUMN_DELETE, "int(1)", false));
        columnsType.put(COLUMN_DELETE, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        Map<String, Set> tmp = new HashMap();
        DbleServer.getInstance().getConfig().getUsers().entrySet().stream().sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).forEach(v -> {
            UserConfig userConfig = v.getValue();
            if (userConfig instanceof ShardingUserConfig) {
                ShardingUserConfig shardingUserConfig = ((ShardingUserConfig) userConfig);
                UserPrivilegesConfig userPrivilegesConfig = shardingUserConfig.getPrivilegesConfig();
                if (!shardingUserConfig.isReadOnly() && userPrivilegesConfig != null && userPrivilegesConfig.isCheck()) {
                    Map<String, UserPrivilegesConfig.SchemaPrivilege> schemaPrivilege = userPrivilegesConfig.getSchemaPrivileges();
                    schemaPrivilege.forEach((schema, sPrivilege) -> {
                        if (shardingUserConfig.getSchemas().contains(schema)) {
                            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema);
                            if (schemaConfig != null) {
                                Set<String> tableMetas = ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getTableMetas().keySet();
                                Map<String, UserPrivilegesConfig.TablePrivilege> tablePrivilege = sPrivilege.getTablePrivileges();

                                String defaultNode = schemaConfig.getShardingNode();
                                Set<String> tables = null;
                                if (defaultNode != null) {
                                    if (tmp.containsKey(schema)) {
                                        tables = tmp.get(schema);
                                    } else {
                                        tables = new ShowTablesHandler().handle(defaultNode);
                                        tmp.put(schema, tables);
                                    }
                                }
                                if (!tablePrivilege.isEmpty()) {
                                    Set<String> table2 = tables;
                                    tablePrivilege.forEach((tableName, tPrivilege) -> {
                                        if (tableMetas.contains(tableName) || (defaultNode != null && !table2.isEmpty() && table2.contains(tableName))) {
                                            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                                            map.put(COLUMN_ID, shardingUserConfig.getId() + "");
                                            map.put(COLUMN_SCHEMA, schema);
                                            map.put(COLUMN_TABLE, tableName);
                                            int[] dml0 = tPrivilege.getDml();
                                            map.put(COLUMN_INSERT, dml0[0] + "");
                                            map.put(COLUMN_UPDATE, dml0[1] + "");
                                            map.put(COLUMN_SELECT, dml0[2] + "");
                                            map.put(COLUMN_DELETE, dml0[3] + "");
                                            list.add(map);
                                        }
                                    });
                                } else {
                                    int[] dml1 = sPrivilege.getDml();
                                    tableMetas.forEach(tableName -> {
                                        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                                        map.put(COLUMN_ID, shardingUserConfig.getId() + "");
                                        map.put(COLUMN_SCHEMA, schema);
                                        map.put(COLUMN_TABLE, tableName);
                                        map.put(COLUMN_INSERT, dml1[0] + "");
                                        map.put(COLUMN_UPDATE, dml1[1] + "");
                                        map.put(COLUMN_SELECT, dml1[2] + "");
                                        map.put(COLUMN_DELETE, dml1[3] + "");
                                        list.add(map);
                                    });

                                    if (defaultNode != null && !tables.isEmpty()) {
                                        tables.forEach(tableName -> {
                                            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                                            map.put(COLUMN_ID, shardingUserConfig.getId() + "");
                                            map.put(COLUMN_SCHEMA, schema);
                                            map.put(COLUMN_TABLE, tableName);
                                            map.put(COLUMN_INSERT, dml1[0] + "");
                                            map.put(COLUMN_UPDATE, dml1[1] + "");
                                            map.put(COLUMN_SELECT, dml1[2] + "");
                                            map.put(COLUMN_DELETE, dml1[3] + "");
                                            list.add(map);
                                        });
                                    }
                                }
                            }
                        }
                    });
                }
            }
        });
        tmp.clear();
        return list.stream().filter(distinctByIdAndSchemaAndTable(p -> p.get(COLUMN_ID) + "_" + p.get(COLUMN_SCHEMA) + "_" + p.get(COLUMN_TABLE))).collect(Collectors.toList());
    }

    public static <T> Predicate<T> distinctByIdAndSchemaAndTable(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    final class ShowTablesHandler {
        private String sql = "SHOW TABLES FROM {0};";
        private String colSuffix = "Tables_in_";
        private Set<String> result;
        private Lock lock;
        private Condition done;
        private boolean finished = false;
        private boolean success = false;

        private ShowTablesHandler() {
            this.lock = new ReentrantLock();
            this.done = lock.newCondition();
        }

        public Set<String> handle(String shardingNode) {
            ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
            PhysicalDbInstance ds = dn.getDbGroup().getWriteDbInstance();
            String colName = colSuffix + dn.getDatabase();
            MultiRowSQLQueryResultHandler resultHandler = new MultiRowSQLQueryResultHandler(new String[]{colName}, new ShowTablesListener(shardingNode, dn.getDatabase(), colName));
            if (ds.isAlive()) {
                SQLJob sqlJob = new SQLJob(sql.replace("{0}", dn.getDatabase()), null, resultHandler, ds);
                sqlJob.run();
            } else {
                SQLJob sqlJob = new SQLJob(sql.replace("{0}", dn.getDatabase()), shardingNode, resultHandler, false);
                sqlJob.run();
            }

            waitDone(dn.getDatabase());

            if (!success) {
                return new HashSet();
            }
            return result;
        }

        private void waitDone(String database) {
            lock.lock();
            try {
                while (!finished) {
                    done.await();
                }
            } catch (InterruptedException e) {
                LOGGER.info("wait 'show tables from " + database + "' grapping done " + e);
            } finally {
                lock.unlock();
            }
        }

        private void signalDone() {
            lock.lock();
            try {
                finished = true;
                done.signal();
            } finally {
                lock.unlock();
            }
        }

        class ShowTablesListener implements SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> {
            private String shardingNode;
            private String colName;
            private String database;

            ShowTablesListener(String shardingNode, String database, String colName) {
                this.shardingNode = shardingNode;
                this.database = database;
                this.colName = colName;
            }

            @Override
            public void onResult(SQLQueryResult<List<Map<String, String>>> res) {
                if (!res.isSuccess()) {
                    LOGGER.warn("execute 'show tables from " + database + "' error in " + shardingNode);
                } else {
                    success = true;
                    result = new HashSet<>();
                    res.getResult().forEach(v -> {
                        result.add(v.get(colName));
                    });
                }
                signalDone();
            }
        }
    }
}
