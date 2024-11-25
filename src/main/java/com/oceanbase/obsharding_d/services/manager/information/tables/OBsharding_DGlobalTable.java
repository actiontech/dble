/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;

import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DGlobalTable extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding-d_global_table";

    private static final String COLUMN_ID = "id";

    private static final String COLUMN_CHECK = "check";

    private static final String COLUMN_CHECK_CLASS = "check_class";

    private static final String COLUMN_CRON = "cron";

    public OBsharding_DGlobalTable() {
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
        return OBsharding_DTable.getTableByType(OBsharding_DTable.TableType.GLOBAL);
    }

}
