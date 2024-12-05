/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.meta.table;

import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.alarm.ToResolveContainer;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableFakeConfig;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.google.common.collect.Maps;

import java.util.*;

public class FakeConfigTableHandler extends ConfigTableHandler {

    private final ProxyMetaManager tmManager;
    private volatile Map<String, Set<String>> recordTableLack = Maps.newHashMap();

    public FakeConfigTableHandler(AbstractSchemaMetaHandler operationalHandler, ProxyMetaManager tmManager) {
        super(operationalHandler);
        this.tmManager = tmManager;
    }

    protected Map<String, BaseTableConfig> getFilterConfigTables(Map<String, BaseTableConfig> configTables, Set<String> filterTableSet) {
        Map<String, BaseTableConfig> newReload = new HashMap<>();
        if (filterTableSet == null) {
            for (Map.Entry<String, BaseTableConfig> entry : schemaConfig.getTables().entrySet())
                if (entry.getValue() instanceof ShardingTableFakeConfig) {
                    newReload.put(entry.getKey(), entry.getValue());
                }
        } else {
            Iterator<String> iterator = filterTableSet.iterator();
            while (iterator.hasNext()) {
                String table = iterator.next();
                if (schemaConfig.getTables().containsKey(table) && schemaConfig.getTables().get(table) instanceof ShardingTableFakeConfig) {
                    newReload.put(table, schemaConfig.getTables().get(table));
                    filterTableSet.remove(table);
                }
            }
        }
        return newReload;
    }

    protected void dealTableLack(String node, String table) {
        Set<String> nodes = recordTableLack.computeIfAbsent(table, v -> new HashSet<>());
        nodes.add(node);
    }

    protected void realDealTableLack() {
        for (Map.Entry<String, Set<String>> entry : recordTableLack.entrySet()) {
            String table = entry.getKey();
            if (entry.getValue().size() == schemaConfig.getDefaultShardingNodes().size()) { // entry.getValue().equals(new TreeSet<>(schemaConfig.getDefaultShardingNodes())
                this.tmManager.dropTable(schema, table);
                String tableId = schema + "." + table;
                LOGGER.warn("found the table[{}] in all defaultNode{} has been lost, will remove his metadata",
                        tableId, schemaConfig.getDefaultShardingNodes());
                for (String shardingNode : entry.getValue()) {
                    ToResolveContainer.TABLE_LACK.remove(AlertUtil.getTableLackKey(shardingNode, table));
                }
                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_SHARDINGS.remove(tableId);
                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY.remove(tableId);
            } else {
                for (String shardingNode : entry.getValue()) {
                    String tableLackKey = AlertUtil.getTableLackKey(shardingNode, table);
                    String warnMsg = "Can't get table " + table + "'s config from shardingNode:" + shardingNode + "! Maybe the table is not initialized!";
                    LOGGER.warn(warnMsg);
                    AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableLackKey));
                    ToResolveContainer.TABLE_LACK.add(tableLackKey);
                }
            }
        }
    }
}
