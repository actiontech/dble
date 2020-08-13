/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.*;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.ProxyMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.*;

public class DbleTable extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_table";

    public static final String COLUMN_ID = "id";

    public static final String COLUMN_NAME = "name";

    public static final String COLUMN_SCHEMA = "schema";

    private static final String COLUMN_MAX_LIMIT = "max_limit";

    public static final String COLUMN_TYPE = "type";

    private static final String COLUMN_GLOBAL_CHECK = "check";

    private static final String COLUMN_GLOBAL_CHECK_CLASS = "check_class";

    private static final String COLUMN_GLOBAL_CRON = "cron";

    private static final String COLUMN_SHARDING_INCREMENT_COLUMN = "increment_column";

    private static final String COLUMN_SHARDING_COLUMN = "sharding_column";

    private static final String COLUMN_SHARDING_SQL_REQUIRED_SHARDING = "sql_required_sharding";

    private static final String COLUMN_SHARDING_ALGORITHM_NAME = "algorithm_name";

    private static final String COLUMN_CHILD_PARENT_ID = "parent_id";

    private static final String COLUMN_CHILD_INCREMENT_COLUMN = "increment_column";

    private static final String COLUMN_CHILD_JOIN_COLUMN = "join_column";

    private static final String COLUMN_CHILD_PAREN_COLUMN = "paren_column";

    public static final String PREFIX_CONFIG = "C";

    public static final String PREFIX_METADATA = "M";

    public DbleTable() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SCHEMA, new ColumnMeta(COLUMN_SCHEMA, "varchar(64)", false));
        columnsType.put(COLUMN_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MAX_LIMIT, new ColumnMeta(COLUMN_MAX_LIMIT, "int(11)", true));
        columnsType.put(COLUMN_MAX_LIMIT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TYPE, new ColumnMeta(COLUMN_TYPE, "varchar(10)", false));
        columnsType.put(COLUMN_TYPE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        return getTableByType(null);
    }

    protected static List<LinkedHashMap<String, String>> getTableByType(TableType tableType) {
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        List<String> nameList = Lists.newArrayList();
        //config
        TreeMap<String, SchemaConfig> schemaMap = new TreeMap<>(DbleServer.getInstance().getConfig().getSchemas());
        if (null != tableType) {
            schemaMap.entrySet().stream().forEach(e -> e.getValue().getTables().entrySet().stream().filter(p -> tableType.equals(distinguishType(p.getValue())) && !nameList.contains(e.getValue().getName() + "-" + p.getKey())).sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).forEach(t -> {
                BaseTableConfig baseTableConfig = t.getValue();
                SchemaConfig schemaConfig = e.getValue();
                LinkedHashMap map = initMap(schemaConfig, baseTableConfig, tableType, null, schemaMap);
                rowList.add(map);
                nameList.add(schemaConfig.getName() + "-" + baseTableConfig.getName());
            }));
        } else {
            schemaMap.entrySet().stream().forEach(e -> e.getValue().getTables().entrySet().stream().filter(p -> !nameList.contains(e.getValue().getName() + "-" + p.getKey())).sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).forEach(t -> {
                SchemaConfig schemaConfig = e.getValue();
                BaseTableConfig baseTableConfig = t.getValue();
                LinkedHashMap map = initMap(schemaConfig, baseTableConfig, null, null, schemaMap);
                rowList.add(map);
                nameList.add(schemaConfig.getName() + "-" + baseTableConfig.getName());
            }));
        }
        if (null != tableType) {
            return rowList;
        }
        //metadata-no sharding
        List<TableMeta> tableMetaList = Lists.newArrayList();
        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        schemaMetaMap.entrySet().stream().forEach(e -> e.getValue().getTableMetas().entrySet().stream().filter(p -> !nameList.contains(e.getKey() + "-" + p.getKey())).forEach(p -> tableMetaList.add(new TableMeta(p.getValue().getId(), e.getKey(), p.getKey()))));
        tableMetaList.stream().sorted(Comparator.comparing(TableMeta::getId)).forEach(e -> {
            TableMeta tableMeta = e;
            LinkedHashMap map = initMap(null, null, null, tableMeta, null);
            rowList.add(map);
            nameList.add(tableMeta.getSchemaName() + "-" + tableMeta.getTableName());
        });
        return rowList;
    }

    /**
     * Set column
     *
     * @param schemaConfig
     * @param baseTableConfig
     * @param tableType
     * @param tableMeta
     * @param schemaMap
     * @return column map
     */
    private static LinkedHashMap initMap(SchemaConfig schemaConfig, BaseTableConfig baseTableConfig, TableType tableType, TableMeta tableMeta, TreeMap<String, SchemaConfig> schemaMap) {
        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
        if (null == tableMeta) {
            //config
            map.put(COLUMN_ID, PREFIX_CONFIG + baseTableConfig.getId());
            switch (tableType == null ? TableType.NO_SHARDING : tableType) {
                case GLOBAL:
                    GlobalTableConfig globalTableConfig = (GlobalTableConfig) baseTableConfig;
                    map.put(COLUMN_GLOBAL_CHECK, String.valueOf(globalTableConfig.isGlobalCheck()));
                    map.put(COLUMN_GLOBAL_CHECK_CLASS, globalTableConfig.getCheckClass());
                    map.put(COLUMN_GLOBAL_CRON, globalTableConfig.getCron());
                    break;
                case SHARDING:
                    ShardingTableConfig shardingTableConfig = (ShardingTableConfig) baseTableConfig;
                    map.put(COLUMN_SHARDING_INCREMENT_COLUMN, shardingTableConfig.getIncrementColumn());
                    map.put(COLUMN_SHARDING_COLUMN, shardingTableConfig.getShardingColumn());
                    map.put(COLUMN_SHARDING_SQL_REQUIRED_SHARDING, String.valueOf(shardingTableConfig.isSqlRequiredSharding()));
                    map.put(COLUMN_SHARDING_ALGORITHM_NAME, shardingTableConfig.getFunction().getName());
                    break;
                case CHILD:
                    ChildTableConfig childTableConfig = (ChildTableConfig) baseTableConfig;
                    BaseTableConfig parentTableConfig = findBySchemaATable(schemaMap, schemaConfig.getName(), childTableConfig.getParentTC().getName());
                    map.put(COLUMN_CHILD_PARENT_ID, null != parentTableConfig ? PREFIX_CONFIG + parentTableConfig.getId() : null);
                    map.put(COLUMN_CHILD_INCREMENT_COLUMN, childTableConfig.getIncrementColumn());
                    map.put(COLUMN_CHILD_JOIN_COLUMN, childTableConfig.getJoinColumn());
                    map.put(COLUMN_CHILD_PAREN_COLUMN, childTableConfig.getParentColumn());
                    break;
                case NO_SHARDING:
                case SINGLE:
                    map.put(COLUMN_NAME, baseTableConfig.getName());
                    map.put(COLUMN_SCHEMA, schemaConfig.getName());
                    map.put(COLUMN_MAX_LIMIT, String.valueOf(baseTableConfig.getMaxLimit()));
                    map.put(COLUMN_TYPE, String.valueOf(distinguishType(baseTableConfig)));
                    break;
                default:
                    break;
            }
        } else {
            //metadata
            map.put(COLUMN_ID, PREFIX_METADATA + tableMeta.getId());
            map.put(COLUMN_NAME, tableMeta.getTableName());
            map.put(COLUMN_SCHEMA, tableMeta.getSchemaName());
            BaseTableConfig tableConfig = DbleServer.getInstance().getConfig().getSchemas().get(tableMeta.getSchemaName()).getTable(tableMeta.getTableName());
            map.put(COLUMN_TYPE, String.valueOf(distinguishType(tableConfig)));
        }
        return map;

    }

    private static BaseTableConfig findBySchemaATable(TreeMap<String, SchemaConfig> schemaMap, String schemaName, String tableName) {
        Map.Entry<String, SchemaConfig> schemaConfigEntry = schemaMap.entrySet().stream().filter(t -> schemaName.equals(t.getKey())).findFirst().get();
        if (null == schemaConfigEntry) {
            return null;
        }
        Map.Entry<String, BaseTableConfig> tableConfigEntry = schemaConfigEntry.getValue().getTables().entrySet().stream().filter(t -> tableName.equals(t.getKey())).findFirst().get();
        if (null == tableConfigEntry) {
            return null;
        }
        return tableConfigEntry.getValue();
    }

    protected static TableType distinguishType(BaseTableConfig tableConfig) {
        if (tableConfig == null) {
            return TableType.NO_SHARDING;
        } else if (tableConfig instanceof GlobalTableConfig) {
            return TableType.GLOBAL;
        } else if (tableConfig instanceof ChildTableConfig) {
            return TableType.CHILD;
        } else if (tableConfig instanceof SingleTableConfig) {
            return TableType.SINGLE;
        } else {
            return TableType.SHARDING;
        }
    }

    public enum TableType {
        GLOBAL, SHARDING, CHILD, NO_SHARDING, SINGLE
    }


}
