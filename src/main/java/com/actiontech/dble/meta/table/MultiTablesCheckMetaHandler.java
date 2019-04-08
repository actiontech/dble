/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.meta.protocol.StructureMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.Set;

/**
 * Created by szf on 2019/4/4.
 */
public class MultiTablesCheckMetaHandler extends MultiTablesMetaHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTablesCheckMetaHandler.class);
    private final ProxyMetaManager tmManager;
    private final String schema;

    public MultiTablesCheckMetaHandler(ProxyMetaManager tmManager, SchemaConfig schemaConfig, Set<String> selfNode) {
        super(schemaConfig, selfNode);
        this.tmManager = tmManager;
        this.schema = schemaConfig.getName();
    }


    @Override
    void handleSingleMetaData(StructureMeta.TableMeta tableMeta) {
        this.checkTableModify(tableMeta);
    }

    @Override
    void schemaMetaFinish() {

    }

    private void checkTableModify(StructureMeta.TableMeta tableMeta) {
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


    private boolean isTableModify(StructureMeta.TableMeta tm) {
        String tbName = tm.getTableName();
        StructureMeta.TableMeta oldTm;
        try {
            oldTm = tmManager.getSyncTableMeta(schema, tbName);
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
        StructureMeta.TableMeta tblMetaTmp = tm.toBuilder().setVersion(oldTm.getVersion()).build();
        if (!oldTm.equals(tblMetaTmp)) { // oldTm!=tblMetaTmp means memory  meta is not equal show create table result
            try {
                StructureMeta.TableMeta test = tmManager.getSyncTableMeta(schema, tbName);
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
