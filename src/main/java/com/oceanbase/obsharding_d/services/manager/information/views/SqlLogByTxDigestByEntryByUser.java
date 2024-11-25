/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.views;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseView;

public class SqlLogByTxDigestByEntryByUser extends ManagerBaseView {

    private static final String VIEW_NAME = "sql_log_by_tx_digest_by_entry_by_user";

    private static final String COLUMN_TX_DIGEST = "tx_digest";
    private static final String COLUMN_EXEC = "exec";
    private static final String COLUMN_ENTRY = "entry";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_SQL_EXEC = "sql_exec";
    private static final String COLUMN_SOURCE_HOST = "source_host";
    private static final String COLUMN_SOURCE_PORT = "source_port";
    private static final String COLUMN_SQL_IDS = "sql_ids";
    private static final String COLUMN_TX_DURATION = "tx_duration";
    private static final String COLUMN_BUSY_TIME = "busy_time";
    private static final String COLUMN_EXAMINED_ROWS = "examined_rows";

    public SqlLogByTxDigestByEntryByUser() {
        super(VIEW_NAME, 11, "select tx_digest,count(tx_digest) exec, user,entry,sum(sql_exec) sql_exec,source_host,source_port,group_concat(sql_ids) sql_ids,sum(tx_duration) tx_duration,sum(busy_time) busy_time,sum(examined_rows) examined_rows from (select group_concat(sql_digest order by sql_id) tx_digest,user,entry,COUNT(sql_id) sql_exec,source_host,source_port,GROUP_CONCAT(sql_id) sql_ids,max(start_time + duration) - min(start_time) tx_duration,sum(duration) busy_time,sum(examined_rows) examined_rows from sql_log group by tx_id) a group by a.tx_digest,a.entry");
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_TX_DIGEST, new ColumnMeta(COLUMN_TX_DIGEST, "varchar(1024)", false));
        columnsType.put(COLUMN_TX_DIGEST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_EXEC, new ColumnMeta(COLUMN_EXEC, "int(11)", false));
        columnsType.put(COLUMN_EXEC, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ENTRY, new ColumnMeta(COLUMN_ENTRY, "int(11)", false));
        columnsType.put(COLUMN_ENTRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(20)", false));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_EXEC, new ColumnMeta(COLUMN_SQL_EXEC, "int(11)", false));
        columnsType.put(COLUMN_SQL_EXEC, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SOURCE_HOST, new ColumnMeta(COLUMN_SOURCE_HOST, "varchar(20)", false));
        columnsType.put(COLUMN_SOURCE_HOST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SOURCE_PORT, new ColumnMeta(COLUMN_SOURCE_PORT, "int(11)", false));
        columnsType.put(COLUMN_SOURCE_PORT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_IDS, new ColumnMeta(COLUMN_SQL_IDS, "varchar(1024)", false));
        columnsType.put(COLUMN_SQL_IDS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TX_DURATION, new ColumnMeta(COLUMN_TX_DURATION, "int(11)", false));
        columnsType.put(COLUMN_TX_DURATION, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_BUSY_TIME, new ColumnMeta(COLUMN_BUSY_TIME, "int(11)", false));
        columnsType.put(COLUMN_BUSY_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_EXAMINED_ROWS, new ColumnMeta(COLUMN_EXAMINED_ROWS, "int(11)", false));
        columnsType.put(COLUMN_EXAMINED_ROWS, Fields.FIELD_TYPE_LONG);
    }
}
