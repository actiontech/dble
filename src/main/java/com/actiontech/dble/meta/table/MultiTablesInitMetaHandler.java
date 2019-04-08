/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.protocol.StructureMeta;

import java.util.*;

/**
 * Created by szf on 2019/4/4.
 */
public class MultiTablesInitMetaHandler extends MultiTablesMetaHandler {
    private String schema;
    private SchemaMetaHandler schemaMetaHandler;


    MultiTablesInitMetaHandler(SchemaMetaHandler schemaMetaHandler, SchemaConfig schemaConfig, Set<String> selfNode) {
        super(schemaConfig, selfNode);
        this.schemaMetaHandler = schemaMetaHandler;
        this.schema = schemaConfig.getName();
    }

    public void execute() {
        this.schemaMetaHandler.getTmManager().createDatabase(schema);
        super.execute();
    }


    public ProxyMetaManager getTmManager() {
        return this.schemaMetaHandler.getTmManager();
    }

    @Override
    void handleSingleMetaData(StructureMeta.TableMeta tableMeta) {
        if (tableMeta != null) {
            this.getTmManager().addTable(schema, tableMeta);
        }
    }


    @Override
    void schemaMetaFinish() {
        schemaMetaHandler.countDown();
    }


}
