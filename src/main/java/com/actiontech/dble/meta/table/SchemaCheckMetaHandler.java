/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.meta.ViewMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Set;

/**
 * Created by szf on 2019/4/4.
 * Only used to table schedule structure check
 * Get latest meta from all shardingNode and just check whether the table meta is the same
 */
public class SchemaCheckMetaHandler extends AbstractSchemaMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCheckMetaHandler.class);
    private final String schema;

    public SchemaCheckMetaHandler(ProxyMetaManager tmManager, SchemaConfig schemaConfig, Set<String> selfNode) {
        super(tmManager, schemaConfig, selfNode, false);
        this.schema = schemaConfig.getName();
    }

    @Override
    void handleSingleMetaData(TableMeta tableMeta) {
        this.checkTableModify(tableMeta);
    }

    @Override
    void handleViewMeta(ViewMeta viewMeta) {
    }

    @Override
    void handleMultiMetaData(Set<TableMeta> tableMetas) {
        for (TableMeta tableMeta : tableMetas) {
            if (tableMeta != null) {
                String tableId = schema + "." + tableMeta.getTableName();
                if (isTableModify(tableMeta)) {
                    String errorMsg = "Table [" + tableMeta.getTableName() + "] are modified by other,Please Check IT!";
                    LOGGER.warn(errorMsg);
                    AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_MEMORY, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                    ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY.add(tableId);
                }
            }
        }
    }

    @Override
    void schemaMetaFinish() {

    }

    private void checkTableModify(TableMeta tableMeta) {
        if (tableMeta != null) {
            String tableId = schema + "." + tableMeta.getTableName();
            if (isTableModify(tableMeta)) {
                String errorMsg = "Table [" + tableMeta.getTableName() + "] are modified by other,Please Check IT!";
                LOGGER.warn(errorMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_MEMORY, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY.add(tableId);
            } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY.contains(tableId)) {
                AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_MEMORY, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                        ToResolveContainer.TABLE_NOT_CONSISTENT_IN_MEMORY, tableId);
            }
            LOGGER.debug("checking table Table [" + tableMeta.getTableName() + "]");
        }
    }


    private boolean isTableModify(TableMeta tm) {
        String tbName = tm.getTableName();
        TableMeta oldTm;
        try {
            oldTm = getTmManager().getSyncTableMeta(schema, tbName);
        } catch (SQLNonTransientException e) {
            //someone ddl, skip.
            return false;
        }
        if (oldTm == null) {
            //the DDL may drop table;
            return false;
        }
        if (oldTm.getVersion() >= tm.getVersion()) {
            //there is an new version TableMeta after check start
            return false;
        }
        TableMeta tblMetaTmp = new TableMeta(tm, oldTm.getVersion());
        if (!oldTm.equals(tblMetaTmp)) { // oldTm!=tblMetaTmp means memory  meta is not equal show create table result
            try {
                TableMeta test = getTmManager().getSyncTableMeta(schema, tbName);
                /* oldTm==test means memory meta is not changed, so memory is really different with show create table result
                  if(oldTm!=test) means memory meta changed ,left to next check
                */
                return oldTm.equals(test);
            } catch (SQLNonTransientException e) {
                //someone ddl, skip.
                return false;
            }
        }
        return false;
    }

}
