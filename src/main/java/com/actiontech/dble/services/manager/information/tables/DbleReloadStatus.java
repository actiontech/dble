package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.meta.ReloadStatus;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.util.FormatUtil;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.actiontech.dble.meta.ReloadStatus.RELOAD_END_NORMAL;
import static com.actiontech.dble.meta.ReloadStatus.RELOAD_INTERRUPUTED;
import static com.actiontech.dble.meta.ReloadStatus.RELOAD_STATUS_NONE;

public class DbleReloadStatus extends ManagerBaseTable {
    private static final String TABLE_NAME = "dble_reload_status";

    private static final String COLUMN_INDEX = "index";
    private static final String COLUMN_CLUSTER = "cluster";
    private static final String COLUMN_RELOAD_TYPE = "reload_type";
    private static final String COLUMN_RELOAD_STATUS = "reload_status";
    private static final String COLUMN_LAST_RELOAD_START = "last_reload_start";
    private static final String COLUMN_LAST_RELOAD_END = "last_reload_end";
    private static final String COLUMN_TRIGGER_TYPE = "trigger_type";
    private static final String COLUMN_END_TYPE = "end_type";

    public DbleReloadStatus() {
        super(TABLE_NAME, 8);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_INDEX, new ColumnMeta(COLUMN_INDEX, "int(11)", false, true));
        columnsType.put(COLUMN_INDEX, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_CLUSTER, new ColumnMeta(COLUMN_CLUSTER, "varchar(20)", false));
        columnsType.put(COLUMN_CLUSTER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_RELOAD_TYPE, new ColumnMeta(COLUMN_RELOAD_TYPE, "varchar(20)", false));
        columnsType.put(COLUMN_RELOAD_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_RELOAD_STATUS, new ColumnMeta(COLUMN_RELOAD_STATUS, "varchar(20)", false));
        columnsType.put(COLUMN_RELOAD_STATUS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_RELOAD_START, new ColumnMeta(COLUMN_LAST_RELOAD_START, "varchar(19)", false));
        columnsType.put(COLUMN_LAST_RELOAD_START, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_RELOAD_END, new ColumnMeta(COLUMN_LAST_RELOAD_END, "varchar(19)", false));
        columnsType.put(COLUMN_LAST_RELOAD_END, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TRIGGER_TYPE, new ColumnMeta(COLUMN_TRIGGER_TYPE, "varchar(20)", false));
        columnsType.put(COLUMN_TRIGGER_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_END_TYPE, new ColumnMeta(COLUMN_END_TYPE, "varchar(20)", false));
        columnsType.put(COLUMN_END_TYPE, Fields.FIELD_TYPE_VAR_STRING);

    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        ReloadStatus status = ReloadManager.getReloadInstance().getStatus();
        List<LinkedHashMap<String, String>> list = new ArrayList();
        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
        if (status == null) {
            map.put(COLUMN_INDEX, "0");
            map.put(COLUMN_CLUSTER, ClusterGeneralConfig.getInstance().getClusterType());
            map.put(COLUMN_RELOAD_TYPE, "");
            map.put(COLUMN_RELOAD_STATUS, RELOAD_STATUS_NONE);
            map.put(COLUMN_LAST_RELOAD_START, "");
            map.put(COLUMN_LAST_RELOAD_END, "");
            map.put(COLUMN_TRIGGER_TYPE, "");
            map.put(COLUMN_END_TYPE, "");
        } else {
            map.put(COLUMN_INDEX, status.getId() + "");
            map.put(COLUMN_CLUSTER, status.getClusterType());
            map.put(COLUMN_RELOAD_TYPE, status.getReloadType() + "");
            map.put(COLUMN_RELOAD_STATUS, status.getStatus());
            map.put(COLUMN_LAST_RELOAD_START, FormatUtil.formatDate(status.getLastReloadStart()));
            map.put(COLUMN_LAST_RELOAD_END, FormatUtil.formatDate(status.getLastReloadEnd()));
            map.put(COLUMN_TRIGGER_TYPE, status.getTriggerType());
            map.put(COLUMN_END_TYPE, status.getLastReloadEnd() != 0 ? (status.isReloadInterrupted() ? RELOAD_INTERRUPUTED : RELOAD_END_NORMAL) : "");
        }
        list.add(map);
        return list;
    }
}
