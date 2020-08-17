/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;

import java.util.LinkedHashMap;
import java.util.List;

public class DbleShardingTable extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_sharding_table";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_INCREMENT_COLUMN = "increment_column";

    private static final String COLUMN_SHARDING_COLUMN = "sharding_column";

    private static final String COLUMN_SQL_REQUIRED_SHARDING = "sql_required_sharding";

    private static final String COLUMN_ALGORITHM_NAME = "algorithm_name";

    public DbleShardingTable() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_INCREMENT_COLUMN, new ColumnMeta(COLUMN_INCREMENT_COLUMN, "varchar(64)", true));
        columnsType.put(COLUMN_INCREMENT_COLUMN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SHARDING_COLUMN, new ColumnMeta(COLUMN_SHARDING_COLUMN, "varchar(64)", false));
        columnsType.put(COLUMN_SHARDING_COLUMN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_REQUIRED_SHARDING, new ColumnMeta(COLUMN_SQL_REQUIRED_SHARDING, "varchar(5)", false));
        columnsType.put(COLUMN_SQL_REQUIRED_SHARDING, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ALGORITHM_NAME, new ColumnMeta(COLUMN_ALGORITHM_NAME, "varchar(32)", false));
        columnsType.put(COLUMN_ALGORITHM_NAME, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        return DbleTable.getTableByType(DbleTable.TableType.SHARDING);
    }

}
