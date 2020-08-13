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

public class DbleGlobalTable extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_global_table";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_CHECK = "check";

    private static final String COLUMN_CHECK_CLASS = "check_class";

    private static final String COLUMN_CRON = "cron";

    public DbleGlobalTable() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "varchar(64)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CHECK, new ColumnMeta(COLUMN_CHECK, "varchar(5)", false));
        columnsType.put(COLUMN_CHECK, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CHECK_CLASS, new ColumnMeta(COLUMN_CHECK_CLASS, "varchar(64)", true));
        columnsType.put(COLUMN_CHECK_CLASS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_CRON, new ColumnMeta(COLUMN_CRON, "varchar(32)", true));
        columnsType.put(COLUMN_CRON, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        return DbleTable.getTableByType(DbleTable.TableType.GLOBAL);
    }

}
