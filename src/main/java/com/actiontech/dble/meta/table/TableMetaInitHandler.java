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
