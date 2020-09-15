package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.*;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.CollectionUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DbleEntryTablePrivilege extends ManagerBaseTable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DbleEntryTablePrivilege.class);

    private static final String TABLE_NAME = "dble_entry_table_privilege";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_SCHEMA = "schema";
    private static final String COLUMN_TABLE = "table";
    private static final String COLUMN_EXIST_METAS = "exist_metas";
    private static final String COLUMN_INSERT = "insert";
    private static final String COLUMN_UPDATE = "update";
    private static final String COLUMN_SELECT = "select";
    private static final String COLUMN_DELETE = "delete";
    private static final String COLUMN_IS_EFFECTIVE = "is_effective";

    public DbleEntryTablePrivilege() {
        super(TABLE_NAME, 9);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "int(11)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SCHEMA, new ColumnMeta(COLUMN_SCHEMA, "varchar(64)", false, true));
        columnsType.put(COLUMN_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TABLE, new ColumnMeta(COLUMN_TABLE, "varchar(64)", false, true));
        columnsType.put(COLUMN_TABLE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_EXIST_METAS, new ColumnMeta(COLUMN_EXIST_METAS, "varchar(5)", false));
        columnsType.put(COLUMN_EXIST_METAS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_INSERT, new ColumnMeta(COLUMN_INSERT, "int(1)", false));
        columnsType.put(COLUMN_INSERT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_UPDATE, new ColumnMeta(COLUMN_UPDATE, "int(1)", false));
        columnsType.put(COLUMN_UPDATE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SELECT, new ColumnMeta(COLUMN_SELECT, "int(1)", false));
        columnsType.put(COLUMN_SELECT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DELETE, new ColumnMeta(COLUMN_DELETE, "int(1)", false));
        columnsType.put(COLUMN_DELETE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_IS_EFFECTIVE, new ColumnMeta(COLUMN_IS_EFFECTIVE, "varchar(5)", false));
        columnsType.put(COLUMN_IS_EFFECTIVE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        DbleServer.getInstance().getConfig().getUsers().entrySet().
                stream().
                sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).
                forEach(v -> {
                    UserConfig userConfig = v.getValue();
                    if (userConfig instanceof ShardingUserConfig) {
                        ShardingUserConfig shardingUserConfig = ((ShardingUserConfig) userConfig);
                        UserPrivilegesConfig userPrivilegesConfig = shardingUserConfig.getPrivilegesConfig();
                        if (userPrivilegesConfig != null) {
                            boolean noEffective = (shardingUserConfig.isReadOnly() || !userPrivilegesConfig.isCheck());
                            Map<String, UserPrivilegesConfig.SchemaPrivilege> schemaPrivilege = userPrivilegesConfig.getSchemaPrivileges();
                            if (!CollectionUtil.isEmpty(schemaPrivilege)) {
                                schemaPrivilege.forEach((schema, sPrivilege) -> {
                                    if (shardingUserConfig.getSchemas().contains(schema)) {
                                        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(schema);
                                        if (schemaConfig != null) {
                                            Set<String> tables = new HashSet<>(schemaConfig.getTables().keySet());
                                            Set<String> metas = new HashSet<>(ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getTableMetas().keySet());
                                            Set<String> viewMetas = ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().keySet();
                                            metas.addAll(viewMetas);
                                            tables.addAll(metas);

                                            Map<String, UserPrivilegesConfig.TablePrivilege> tablePrivilege = sPrivilege.getTablePrivileges();
                                            if (!CollectionUtil.isEmpty(tablePrivilege)) {
                                                tablePrivilege.forEach((tableName, tPrivilege) -> {
                                                    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                                                    map.put(COLUMN_ID, shardingUserConfig.getId() + "");
                                                    map.put(COLUMN_SCHEMA, schema);
                                                    map.put(COLUMN_TABLE, tableName);
                                                    map.put(COLUMN_EXIST_METAS, metas.contains(tableName) + "");
                                                    int[] dml0 = tPrivilege.getDml();
                                                    map.put(COLUMN_INSERT, dml0[0] + "");
                                                    map.put(COLUMN_UPDATE, dml0[1] + "");
                                                    map.put(COLUMN_SELECT, dml0[2] + "");
                                                    map.put(COLUMN_DELETE, dml0[3] + "");
                                                    map.put(COLUMN_IS_EFFECTIVE, ((viewMetas.contains(tableName) && !schemaConfig.isNoSharding()) ? false : (noEffective ? false : tables.contains(tableName))) + "");
                                                    list.add(map);
                                                });
                                            } else {
                                                int[] dml1 = sPrivilege.getDml();
                                                tables.forEach(tableName -> {
                                                    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                                                    map.put(COLUMN_ID, shardingUserConfig.getId() + "");
                                                    map.put(COLUMN_SCHEMA, schema);
                                                    map.put(COLUMN_TABLE, tableName);
                                                    map.put(COLUMN_EXIST_METAS, metas.contains(tableName) + "");
                                                    map.put(COLUMN_INSERT, dml1[0] + "");
                                                    map.put(COLUMN_UPDATE, dml1[1] + "");
                                                    map.put(COLUMN_SELECT, dml1[2] + "");
                                                    map.put(COLUMN_DELETE, dml1[3] + "");
                                                    map.put(COLUMN_IS_EFFECTIVE, ((viewMetas.contains(tableName) && !schemaConfig.isNoSharding()) ? false : (noEffective ? false : metas.contains(tableName))) + "");
                                                    list.add(map);
                                                });
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
        return list;
    }
}
