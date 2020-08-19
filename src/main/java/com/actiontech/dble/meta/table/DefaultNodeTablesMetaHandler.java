/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.meta.ViewMeta;

import java.util.Set;

public class DefaultNodeTablesMetaHandler extends GetTableMetaHandler {

    private AbstractSchemaMetaHandler schemaMetaHandler;

    DefaultNodeTablesMetaHandler(AbstractSchemaMetaHandler schemaMetaHandler, String schema, boolean isReload) {
        super(schema, isReload);
        this.schemaMetaHandler = schemaMetaHandler;
    }

    @Override
    void handleTable(String shardingNode, String table, boolean isView, String createSQL) {
        if (isView) {
            ViewMeta viewMeta = MetaHelper.initViewMeta(schema, createSQL, System.currentTimeMillis(), schemaMetaHandler.getTmManager());
            schemaMetaHandler.handleViewMeta(viewMeta);
        } else {
            TableMeta tableMeta = MetaHelper.initTableMeta(table, createSQL, System.currentTimeMillis(), schema);
            schemaMetaHandler.handleSingleMetaData(tableMeta);
        }
    }

    @Override
    void countdown(String shardingNode, Set<String> remainingTables) {
        if (remainingTables.size() > 0) {
            for (String table : remainingTables) {
                logger.warn("show create table " + table + " has no results");
            }
        }
        schemaMetaHandler.countDownSingleTable();
    }
}
