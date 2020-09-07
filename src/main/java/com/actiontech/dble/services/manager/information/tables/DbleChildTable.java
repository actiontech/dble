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

public class DbleChildTable extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_child_table";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_PARENT_ID = "parent_id";

    private static final String COLUMN_INCREMENT_COLUMN = "increment_column";

    private static final String COLUMN_JOIN_COLUMN = "join_column";

    private static final String COLUMN_PAREN_COLUMN = "paren_column";

    public DbleChildTable() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PARENT_ID, new ColumnMeta(COLUMN_PARENT_ID, "varchar(64)", false));
        columnsType.put(COLUMN_PARENT_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_INCREMENT_COLUMN, new ColumnMeta(COLUMN_INCREMENT_COLUMN, "varchar(64)", true));
        columnsType.put(COLUMN_INCREMENT_COLUMN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_JOIN_COLUMN, new ColumnMeta(COLUMN_JOIN_COLUMN, "varchar(64)", false));
        columnsType.put(COLUMN_JOIN_COLUMN, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PAREN_COLUMN, new ColumnMeta(COLUMN_PAREN_COLUMN, "varchar(64)", false));
        columnsType.put(COLUMN_PAREN_COLUMN, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        return DbleTable.getTableByType(DbleTable.TableType.CHILD);
    }

}
