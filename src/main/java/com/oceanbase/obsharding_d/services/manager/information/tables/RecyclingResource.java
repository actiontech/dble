/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.PooledConnection;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;

public class RecyclingResource extends ManagerBaseTable {

    private static final String TABLE_NAME = "recycling_resource";

    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_INFO = "info";

    public RecyclingResource() {
        super(TABLE_NAME, 2);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_TYPE, new ColumnMeta(COLUMN_TYPE, "varchar(64)", false));
        columnsType.put(COLUMN_TYPE, Fields.FIELD_TYPE_VAR_STRING);
        columns.put(COLUMN_INFO, new ColumnMeta(COLUMN_INFO, "text", false));
        columnsType.put(COLUMN_INFO, Fields.FIELD_TYPE_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> result = Lists.newArrayList();
        //dbGroup
        for (PhysicalDbGroup dbGroup : IOProcessor.BACKENDS_OLD_GROUP) {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_TYPE, "dbGroup");
            map.put(COLUMN_INFO, dbGroup.toString());
            result.add(map);
        }
        //dbInstance
        for (PhysicalDbInstance dbInstance : IOProcessor.BACKENDS_OLD_INSTANCE) {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_TYPE, "dbInstance");
            map.put(COLUMN_INFO, dbInstance.toString());
            result.add(map);
        }
        //backendConnection
        for (PooledConnection connection : IOProcessor.BACKENDS_OLD) {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_TYPE, "backendConnection");
            map.put(COLUMN_INFO, connection.toString());
            result.add(map);
        }
        return result;
    }
}
