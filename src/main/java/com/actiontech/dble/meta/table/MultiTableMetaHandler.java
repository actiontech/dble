/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.ProxyMetaManager;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiTableMetaHandler {
    private AtomicInteger tableNumbers;
    private String schema;
    private SchemaConfig config;
    private SchemaMetaHandler schemaMetaHandler;
    private Set<String> selfNode;

    public MultiTableMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig config, Set<String> selfNode) {
        this.schemaMetaHandler = schemaMetaHandler;
        this.config = config;
        this.schema = config.getName();
        this.selfNode = selfNode;
        tableNumbers = new AtomicInteger(config.getTables().size());

    }

    public void execute() {
        this.schemaMetaHandler.getTmManager().createDatabase(schema);
        if (config.getTables().size() == 0) {
            schemaMetaHandler.countDown();
            return;
        }
        for (Entry<String, TableConfig> entry : config.getTables().entrySet()) {
            AbstractTableMetaHandler table = new TableMetaInitHandler(this, schema, entry.getValue(), selfNode);
            table.execute();
        }

    }

    public void countDown() {
        if (tableNumbers.decrementAndGet() == 0)
            schemaMetaHandler.countDown();
    }

    public ProxyMetaManager getTmManager() {
        return this.schemaMetaHandler.getTmManager();
    }
}
