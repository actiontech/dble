package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.ProxyMeta;
import com.google.common.collect.Maps;

import java.util.*;

public class DbleDdlLock extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_ddl_lock";

    private static final String COLUMN_SCHEMA = "schema";
    private static final String COLUMN_TABLE = "table";
    private static final String COLUMN_SQL = "sql";

    public DbleDdlLock() {
        super(TABLE_NAME, 3);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_SCHEMA, new ColumnMeta(COLUMN_SCHEMA, "varchar(64)", false, true));
        columnsType.put(COLUMN_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TABLE, new ColumnMeta(COLUMN_TABLE, "varchar(64)", false, true));
        columnsType.put(COLUMN_TABLE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL, new ColumnMeta(COLUMN_SQL, "varchar(500)", false));
        columnsType.put(COLUMN_SQL, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        ProxyMeta.getInstance().getTmManager().getLockTables().forEach((k, v) -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            String[] infos = k.split("\\.");
            map.put(COLUMN_SCHEMA, infos[0]);
            map.put(COLUMN_TABLE, infos[1]);
            map.put(COLUMN_SQL, v.length() <= 1024 ? v : v.substring(0, 1024));
            list.add(map);
        });
        return list;
    }
}
