package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class DbleProcessor extends ManagerBaseTable {

    private static final String SESSION_TYPE = "session";
    private static final String BACKEND_TYPE = "backend";

    private static final String TABLE_NAME = "dble_processor";

    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_CONN_COUNT = "conn_count";
    private static final String COLUMN_CONN_NET_IN = "conn_net_in";
    private static final String COLUMN_CONN_NET_OUT = "conn_net_out";

    public DbleProcessor() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {

        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TYPE, new ColumnMeta(COLUMN_TYPE, "varchar(7)", false));
        columnsType.put(COLUMN_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CONN_COUNT, new ColumnMeta(COLUMN_CONN_COUNT, "int(11)", false));
        columnsType.put(COLUMN_CONN_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_CONN_NET_IN, new ColumnMeta(COLUMN_CONN_NET_IN, "int(11)", false));
        columnsType.put(COLUMN_CONN_NET_IN, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_CONN_NET_OUT, new ColumnMeta(COLUMN_CONN_NET_OUT, "int(11)", false));
        columnsType.put(COLUMN_CONN_NET_OUT, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();

        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, p.getName());
            map.put(COLUMN_TYPE, SESSION_TYPE);
            map.put(COLUMN_CONN_COUNT, p.getFrontends().size() + "");
            map.put(COLUMN_CONN_NET_IN, p.getNetInBytes() + "");
            map.put(COLUMN_CONN_NET_OUT, p.getNetOutBytes() + "");
            list.add(map);
        }
        for (IOProcessor p : DbleServer.getInstance().getBackendProcessors()) {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_NAME, p.getName());
            map.put(COLUMN_TYPE, BACKEND_TYPE);
            map.put(COLUMN_CONN_COUNT, p.getBackends().size() + "");
            map.put(COLUMN_CONN_NET_IN, p.getNetInBytes() + "");
            map.put(COLUMN_CONN_NET_OUT, p.getNetOutBytes() + "");
            list.add(map);
        }
        return list;
    }
}
