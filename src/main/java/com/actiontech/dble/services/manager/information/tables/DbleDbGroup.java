/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.*;
import java.util.stream.Collectors;

public class DbleDbGroup extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_db_group";

    private static final String COLUMN_NAME = "name";

    private static final String COLUMN_HEARTBEAT_STMT = "heartbeat_stmt";

    private static final String COLUMN_HEARTBEAT_TIMEOUT = "heartbeat_timeout";

    private static final String COLUMN_HEARTBEAT_RETRY = "heartbeat_retry";

    private static final String COLUMN_RW_SPLIT_MODE = "rw_split_mode";

    private static final String COLUMN_DELAY_THRESHOLD = "delay_threshold";

    private static final String COLUMN_DISABLE_HA = "disable_ha";

    public DbleDbGroup() {
        super(TABLE_NAME, 7);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HEARTBEAT_STMT, new ColumnMeta(COLUMN_HEARTBEAT_STMT, "varchar(64)", false));
        columnsType.put(COLUMN_HEARTBEAT_STMT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HEARTBEAT_TIMEOUT, new ColumnMeta(COLUMN_HEARTBEAT_TIMEOUT, "int(11)", true));
        columnsType.put(COLUMN_HEARTBEAT_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_HEARTBEAT_RETRY, new ColumnMeta(COLUMN_HEARTBEAT_RETRY, "int(11)", true));
        columnsType.put(COLUMN_HEARTBEAT_RETRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_RW_SPLIT_MODE, new ColumnMeta(COLUMN_RW_SPLIT_MODE, "int(11)", false));
        columnsType.put(COLUMN_RW_SPLIT_MODE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DELAY_THRESHOLD, new ColumnMeta(COLUMN_DELAY_THRESHOLD, "int(11)", true));
        columnsType.put(COLUMN_DELAY_THRESHOLD, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DISABLE_HA, new ColumnMeta(COLUMN_DISABLE_HA, "varchar(5)", true));
        columnsType.put(COLUMN_DISABLE_HA, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, PhysicalDbGroup> dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups();
        return dbGroupMap.values().stream().map(dbGroup -> {
            DbGroupConfig dbGroupConfig = dbGroup.getDbGroupConfig();
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, dbGroup.getGroupName());
            map.put(COLUMN_HEARTBEAT_STMT, null == dbGroupConfig ? null : dbGroupConfig.getHeartbeatSQL());
            map.put(COLUMN_HEARTBEAT_TIMEOUT, null == dbGroupConfig ? null : String.valueOf(dbGroupConfig.getHeartbeatTimeout()));
            map.put(COLUMN_HEARTBEAT_RETRY, null == dbGroupConfig ? null : String.valueOf(dbGroupConfig.getErrorRetryCount()));
            map.put(COLUMN_RW_SPLIT_MODE, String.valueOf(dbGroup.getRwSplitMode()));
            map.put(COLUMN_DELAY_THRESHOLD, String.valueOf(dbGroupConfig.getDelayThreshold()));
            map.put(COLUMN_DISABLE_HA, String.valueOf(dbGroupConfig.isDisableHA()));
            return map;
        }).collect(Collectors.toList());
    }
}
