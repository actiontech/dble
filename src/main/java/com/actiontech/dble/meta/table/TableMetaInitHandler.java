/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;

import java.util.Set;

public class TableMetaInitHandler extends AbstractTableMetaHandler {
    private MultiTableMetaHandler multiTableMetaHandler;

    public TableMetaInitHandler(MultiTableMetaHandler multiTableMetaHandler, String schema, TableConfig tbConfig, Set<String> selfNode) {
        super(schema, tbConfig, selfNode);
        this.multiTableMetaHandler = multiTableMetaHandler;
    }

    @Override
    protected void countdown() {
        multiTableMetaHandler.countDown();
    }

    @Override
    protected void handlerTable(StructureMeta.TableMeta tableMeta) {
        multiTableMetaHandler.getTmManager().addTable(schema, tableMeta);
    }

}
