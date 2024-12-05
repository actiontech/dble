/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;

import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DShardingTable extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding_d_sharding_table";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_INCREMENT_COLUMN = "increment_column";

    private static final String COLUMN_SHARDING_COLUMN = "sharding_column";

    private static final String COLUMN_SQL_REQUIRED_SHARDING = "sql_required_sharding";

    private static final String COLUMN_ALGORITHM_NAME = "algorithm_name";

    public OBsharding_DShardingTable() {
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
        return OBsharding_DTable.getTableByType(OBsharding_DTable.TableType.SHARDING);
    }

}
