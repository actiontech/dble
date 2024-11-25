/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.google.common.collect.Maps;

import java.util.*;

public class OBsharding_DDdlLock extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding-d_ddl_lock";

    private static final String COLUMN_SCHEMA = "schema";
    private static final String COLUMN_TABLE = "table";
    private static final String COLUMN_SQL = "sql";

    public OBsharding_DDdlLock() {
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
