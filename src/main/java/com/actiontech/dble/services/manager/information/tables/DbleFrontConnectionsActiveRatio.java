package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.statistic.stat.FrontActiveRatioStat;
import com.google.common.collect.Maps;

import java.util.*;


public class DbleFrontConnectionsActiveRatio extends ManagerBaseTable {

    public DbleFrontConnectionsActiveRatio() {
        super("session_connections_active_ratio", 4);
    }

    private static final String COLUMN_SESSION_CONN_ID = "session_conn_id";

    private static final String COLUMN_LAST_HALF_MIN = "last_half_minute";

    private static final String COLUMN_LAST_MINUTE = "last_minute";

    private static final String COLUMN_LAST_FIVE_MINUTE = "last_five_minute";

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_SESSION_CONN_ID, new ColumnMeta(COLUMN_SESSION_CONN_ID, "int(11)", false, true));
        columnsType.put(COLUMN_SESSION_CONN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_LAST_HALF_MIN, new ColumnMeta(COLUMN_LAST_HALF_MIN, "varchar(5)", false));
        columnsType.put(COLUMN_LAST_HALF_MIN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_MINUTE, new ColumnMeta(COLUMN_LAST_MINUTE, "varchar(5)", false));
        columnsType.put(COLUMN_LAST_MINUTE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_FIVE_MINUTE, new ColumnMeta(COLUMN_LAST_FIVE_MINUTE, "varchar(5)", false));
        columnsType.put(COLUMN_LAST_FIVE_MINUTE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<FrontendConnection, String[]> statMaps = FrontActiveRatioStat.getInstance().getActiveRatioStat();
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        statMaps.entrySet().stream().sorted(Comparator.comparingLong(i -> i.getKey().getId())).forEach(w -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_SESSION_CONN_ID, w.getKey().getId() + "");
            map.put(COLUMN_LAST_HALF_MIN, w.getValue()[0]);
            map.put(COLUMN_LAST_MINUTE, w.getValue()[1]);
            map.put(COLUMN_LAST_FIVE_MINUTE, w.getValue()[2]);
            list.add(map);
        });
        return list;
    }
}
