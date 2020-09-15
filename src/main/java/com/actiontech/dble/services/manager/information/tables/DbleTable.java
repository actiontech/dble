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

        for (Map.Entry<String, SchemaConfig> schemaConfigEntry : schemaMap.entrySet()) {
            SchemaConfig schemaConfig = schemaConfigEntry.getValue();
            Map<String, BaseTableConfig> tableConfigMap = schemaConfig.getTables();
            List<Map.Entry<String, BaseTableConfig>> tableConfigList = new ArrayList<>(tableConfigMap.entrySet());
            tableConfigList.sort(Comparator.comparingInt(tableConfigEntry -> tableConfigEntry.getValue().getId()));
            for (Map.Entry<String, BaseTableConfig> tableConfigEntry : tableConfigList) {
                BaseTableConfig tableConfig = tableConfigEntry.getValue();
                if (null != tableType && tableType.equals(distinguishType(tableConfig)) && !nameList.contains(schemaConfig.getName() + "-" + tableConfig.getName())) {
                    // dble_global, dble_sharding, dble_child
                    LinkedHashMap<String, String> map = initMap(schemaConfig, tableConfig, tableType, null, schemaMap);
                    rowList.add(map);
                    nameList.add(schemaConfig.getName() + "-" + tableConfig.getName());
                } else if (null == tableType && !nameList.contains(schemaConfig.getName() + "-" + tableConfig.getName())) {
                    //dble_table
                    LinkedHashMap<String, String> map = initMap(schemaConfig, tableConfig, null, null, schemaMap);
                    rowList.add(map);
                    nameList.add(schemaConfig.getName() + "-" + tableConfig.getName());
                }
            }
        }
        if (null != tableType) {
            return rowList;
        }
        //metadata-no sharding
        List<TableMeta> tableMetaList = Lists.newArrayList();
        Map<String, SchemaMeta> schemaMetaMap = ProxyMeta.getInstance().getTmManager().getCatalogs();
        for (Map.Entry<String, SchemaMeta> schemaMetaEntry : schemaMetaMap.entrySet()) {
            String schemaName = schemaMetaEntry.getKey();
            SchemaMeta schemaMeta = schemaMetaEntry.getValue();
            Map<String, TableMeta> tableMetaMap = schemaMeta.getTableMetas();
            for (Map.Entry<String, TableMeta> tableMetaEntry : tableMetaMap.entrySet()) {
                String tableName = tableMetaEntry.getKey();
                TableMeta tableMeta = tableMetaEntry.getValue();
                if (!nameList.contains(schemaName + "-" + tableName)) {
                    tableMetaList.add(new TableMeta(tableMeta.getId(), schemaName, tableName));
                }
            }
        }
        tableMetaList.sort(Comparator.comparing(TableMeta::getId));
        tableMetaList.forEach(tableMeta -> {
            LinkedHashMap<String, String> map = initMap(null, null, null, tableMeta, null);
            rowList.add(map);
            nameList.add(tableMeta.getSchemaName() + "-" + tableMeta.getTableName());
        });
        return rowList;
    }

    /**
     * Set column
     * tableMeta is null,use config. if not use tableMeta
     *
     * @param schemaConfig    schema_config
     * @param baseTableConfig table_config
     * @param tableType       table_type
     * @param tableMeta       table_metadata
     * @param schemaMap       source schema data
     * @return column map
     */
    private static LinkedHashMap<String, String> initMap(SchemaConfig schemaConfig, BaseTableConfig baseTableConfig, TableType tableType, TableMeta tableMeta, TreeMap<String, SchemaConfig> schemaMap) {
        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
        if (null == tableMeta && null != baseTableConfig && null != schemaConfig) {
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
        } else if (null != tableMeta) {
            //metadata
            SchemaConfig metadataSchema = DbleServer.getInstance().getConfig().getSchemas().get(tableMeta.getSchemaName());
            BaseTableConfig tableConfig = metadataSchema.getTable(tableMeta.getTableName());
            map.put(COLUMN_ID, PREFIX_METADATA + tableMeta.getId());
            map.put(COLUMN_NAME, tableMeta.getTableName());
            map.put(COLUMN_SCHEMA, tableMeta.getSchemaName());
            map.put(COLUMN_MAX_LIMIT, String.valueOf(metadataSchema.getDefaultMaxLimit()));
            map.put(COLUMN_TYPE, String.valueOf(distinguishType(tableConfig)));
        }
        return map;

    }

    private static BaseTableConfig findBySchemaATable(TreeMap<String, SchemaConfig> schemaMap, String schemaName, String tableName) {
        Optional<Map.Entry<String, SchemaConfig>> schemaConfigEntryOptional = schemaMap.entrySet().stream().filter(t -> schemaName.equals(t.getKey())).findFirst();
        if (!schemaConfigEntryOptional.isPresent()) {
            return null;
        }
        Map.Entry<String, SchemaConfig> schemaConfigEntry = schemaConfigEntryOptional.get();
        Map<String, BaseTableConfig> tableMap = schemaConfigEntry.getValue().getTables();
        Optional<Map.Entry<String, BaseTableConfig>> tableConfigEntryOptional = tableMap.entrySet().stream().filter(t -> tableName.equals(t.getKey())).findFirst();
        if (!tableConfigEntryOptional.isPresent()) {
            return null;
        }
        Map.Entry<String, BaseTableConfig> tableConfigEntry = tableConfigEntryOptional.get();
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
