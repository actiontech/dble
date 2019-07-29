/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.SchemaConfig;

import java.util.ArrayList;
import java.util.List;

public class GetSchemaDefaultNodeTablesHandler extends GetNodeTablesHandler {

    private SchemaConfig config;
    private MultiTablesMetaHandler multiTablesMetaHandler;
    private volatile boolean finished = false;

    GetSchemaDefaultNodeTablesHandler(MultiTablesMetaHandler multiTablesMetaHandler, SchemaConfig config) {
        super(config.getDataNode());
        this.multiTablesMetaHandler = multiTablesMetaHandler;
        this.config = config;
    }

    private volatile List<String> tables = new ArrayList<>();

    public List<String> getTables() {
        return tables;
    }

    @Override
    protected void handleTables(String table) {
        if (!config.getTables().containsKey(table)) {
            tables.add(table);
        }
    }

    @Override
    protected void handleFinished() {
        finished = true;
        multiTablesMetaHandler.showTablesFinished();
    }

    public boolean isFinished() {
        return finished;
    }
}
