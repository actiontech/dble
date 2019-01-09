/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table.old;

import com.actiontech.dble.meta.protocol.StructureMeta;

import java.util.List;
import java.util.Set;

public class SingleTableMetaInitHandler extends AbstractTableMetaHandler {
    private MultiTableMetaHandler multiTableMetaHandler;

    SingleTableMetaInitHandler(MultiTableMetaHandler multiTableMetaHandler, String schema, String tableName, List<String> dataNodes, Set<String> selfNode) {
        super(schema, tableName, dataNodes, selfNode);
        this.multiTableMetaHandler = multiTableMetaHandler;
    }

    @Override
    protected void countdown() {
        multiTableMetaHandler.countDownSingleTable();
    }

    @Override
    protected void handlerTable(StructureMeta.TableMeta tableMeta) {
        if (tableMeta != null) {
            multiTableMetaHandler.getTmManager().addTable(schema, tableMeta);
        }
    }
}
