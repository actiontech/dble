/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import java.util.Map;
import java.util.Set;

public class TablesMetaInitHandler extends AbstractTablesMetaHandler {
    private MultiTablesMetaHandler multiTablesMetaHandler;

    public TablesMetaInitHandler(MultiTablesMetaHandler multiTablesMetaHandler, String schema, Map<String, Set<String>> dataNodeMap, Set<String> selfNode) {
        super(schema, dataNodeMap, selfNode);
        this.multiTablesMetaHandler = multiTablesMetaHandler;
    }

    @Override
    protected void countdown() {
        multiTablesMetaHandler.countDownShardTable();
    }

    @Override
    protected void handlerTable(String table, String dataNode, String sql) {
        multiTablesMetaHandler.checkTableConsistent(table, dataNode, sql);
    }

}
