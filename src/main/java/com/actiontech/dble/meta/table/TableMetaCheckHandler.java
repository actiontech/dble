/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.protocol.StructureMeta;

import java.util.Set;

public class TableMetaCheckHandler extends AbstractTableMetaHandler {
    private final ProxyMetaManager tmManager;

    public TableMetaCheckHandler(ProxyMetaManager tmManager, String schema, TableConfig tbConfig, Set<String> selfNode) {
        super(schema, tbConfig, selfNode);
        this.tmManager = tmManager;
    }

    @Override
    protected void countdown() {
    }

    @Override
    protected void handlerTable(StructureMeta.TableMeta tableMeta) {
        if (isTableModify(schema, tableMeta)) {
            LOGGER.warn("Table [" + tableMeta.getTableName() + "] are modified by other,Please Check IT!");
        }
        LOGGER.debug("checking table Table [" + tableMeta.getTableName() + "]");
    }

    private boolean isTableModify(String schema, StructureMeta.TableMeta tm) {
        String tbName = tm.getTableName();
        StructureMeta.TableMeta oldTm = tmManager.getSyncTableMeta(schema, tbName);
        if (oldTm == null) {
            //the DDL may drop table;
            return false;
        }
        if (oldTm.getVersion() >= tm.getVersion()) {
            //there is an new version TableMeta after check start
            return false;
        }
        StructureMeta.TableMeta tblMetaTmp = tm.toBuilder().setVersion(oldTm.getVersion()).build();
        //TODO: thread not safe
        return !oldTm.equals(tblMetaTmp) && oldTm.equals(tmManager.getSyncTableMeta(schema, tbName));
    }
}
