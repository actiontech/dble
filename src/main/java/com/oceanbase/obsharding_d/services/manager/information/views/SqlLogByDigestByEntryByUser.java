/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.views;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseView;

public class SqlLogByDigestByEntryByUser extends ManagerBaseView {

    private static final String VIEW_NAME = "sql_log_by_digest_by_entry_by_user";

    private static final String COLUMN_TX_ID = "sql_digest";
    private static final String COLUMN_ENTRY = "entry";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_EXEC = "exec";
    private static final String COLUMN_DURATION = "duration";
    private static final String COLUMN_ROWS = "rows";
    private static final String COLUMN_EXAMINED_ROWS = "examined_rows";
    private static final String COLUMN_AVG_DURATION = "avg_duration";

    public SqlLogByDigestByEntryByUser() {
        super(VIEW_NAME, 8, "select sql_digest,entry,user,COUNT(sql_id) exec,sum(duration) duration,sum(rows) rows,sum(examined_rows) examined_rows,duration / COUNT(sql_id) avg_duration from sql_log group by sql_digest,entry");
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_TX_ID, new ColumnMeta(COLUMN_TX_ID, "int(11)", false));
        columnsType.put(COLUMN_TX_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ENTRY, new ColumnMeta(COLUMN_ENTRY, "int(11)", false));
        columnsType.put(COLUMN_ENTRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(20)", false));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_EXEC, new ColumnMeta(COLUMN_EXEC, "int(11)", false));
        columnsType.put(COLUMN_EXEC, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DURATION, new ColumnMeta(COLUMN_DURATION, "int(11)", false));
        columnsType.put(COLUMN_DURATION, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ROWS, new ColumnMeta(COLUMN_ROWS, "int(11)", false));
        columnsType.put(COLUMN_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_EXAMINED_ROWS, new ColumnMeta(COLUMN_EXAMINED_ROWS, "int(11)", false));
        columnsType.put(COLUMN_EXAMINED_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_AVG_DURATION, new ColumnMeta(COLUMN_AVG_DURATION, "int(11)", false));
        columnsType.put(COLUMN_AVG_DURATION, Fields.FIELD_TYPE_LONG);
    }
}
