/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables.statistic;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.statistic.sql.handler.SqlStatisticHandler;
import com.oceanbase.obsharding_d.statistic.sql.handler.StatisticDataHandler;
import com.oceanbase.obsharding_d.util.SqlStringUtil;
import com.google.common.collect.Maps;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SqlLog extends ManagerBaseTable {
    public static final String TABLE_NAME = "sql_log";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

    private static final String COLUMN_SQL_ID = "sql_id";
    private static final String COLUMN_SQL_STMT = "sql_stmt";
    private static final String COLUMN_SQL_DIGEST = "sql_digest";
    private static final String COLUMN_SQL_TYPE = "sql_type";
    private static final String COLUMN_TX_ID = "tx_id";
    private static final String COLUMN_ENTRY = "entry";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_SOURCE_HOST = "source_host";
    private static final String COLUMN_SOURCE_PORT = "source_port";
    private static final String COLUMN_ROWS = "rows";
    private static final String COLUMN_EXAMINED_ROWS = "examined_rows";
    private static final String COLUMN_START_TIME = "start_time";
    private static final String COLUMN_DURATION = "duration";

    public SqlLog() {
        super(TABLE_NAME, 12);
        useTruncate();
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_SQL_ID, new ColumnMeta(COLUMN_SQL_ID, "int(11)", false, true));
        columnsType.put(COLUMN_SQL_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_STMT, new ColumnMeta(COLUMN_SQL_STMT, "varchar(1024)", false));
        columnsType.put(COLUMN_SQL_STMT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_DIGEST, new ColumnMeta(COLUMN_SQL_DIGEST, "varchar(1024)", false));
        columnsType.put(COLUMN_SQL_DIGEST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_TYPE, new ColumnMeta(COLUMN_SQL_TYPE, "varchar(16)", false));
        columnsType.put(COLUMN_SQL_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TX_ID, new ColumnMeta(COLUMN_TX_ID, "int(11)", false, true));
        columnsType.put(COLUMN_TX_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ENTRY, new ColumnMeta(COLUMN_ENTRY, "int(11)", false));
        columnsType.put(COLUMN_ENTRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(20)", false));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SOURCE_HOST, new ColumnMeta(COLUMN_SOURCE_HOST, "varchar(20)", false));
        columnsType.put(COLUMN_SOURCE_HOST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SOURCE_PORT, new ColumnMeta(COLUMN_SOURCE_PORT, "int(11)", false));
        columnsType.put(COLUMN_SOURCE_PORT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ROWS, new ColumnMeta(COLUMN_ROWS, "int(11)", false));
        columnsType.put(COLUMN_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_EXAMINED_ROWS, new ColumnMeta(COLUMN_EXAMINED_ROWS, "int(11)", false));
        columnsType.put(COLUMN_EXAMINED_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_START_TIME, new ColumnMeta(COLUMN_START_TIME, "timestamp", false));
        columnsType.put(COLUMN_START_TIME, Fields.FIELD_TYPE_TIMESTAMP);

        columns.put(COLUMN_DURATION, new ColumnMeta(COLUMN_DURATION, "int(11)", false));
        columnsType.put(COLUMN_DURATION, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        StatisticDataHandler dataHandler = StatisticManager.getInstance().getHandler(TABLE_NAME);
        if (dataHandler == null) {
            return Collections.emptyList();
        }

        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        LinkedList<SqlStatisticHandler.TxRecord> txs = (LinkedList<SqlStatisticHandler.TxRecord>) dataHandler.getList();

        txs.forEach(txRecord -> txRecord.getSqls().forEach(sqlRecord -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_SQL_ID, sqlRecord.getSqlId() + "");
            if (sqlRecord.getStmt().length() > 1024) {
                map.put(COLUMN_SQL_STMT, sqlRecord.getStmt().substring(0, 1024) + "...");
            } else {
                map.put(COLUMN_SQL_STMT, sqlRecord.getStmt());
            }
            if (sqlRecord.getSqlDigest().length() > 1024) {
                map.put(COLUMN_SQL_DIGEST, sqlRecord.getSqlDigest().substring(0, 1024) + "...");
            } else {
                map.put(COLUMN_SQL_DIGEST, sqlRecord.getSqlDigest());
            }
            map.put(COLUMN_SQL_TYPE, SqlStringUtil.getSqlType(sqlRecord.getSqlType()));
            map.put(COLUMN_TX_ID, sqlRecord.getTxId() + "");
            map.put(COLUMN_ENTRY, sqlRecord.getEntry() + "");
            map.put(COLUMN_USER, sqlRecord.getUser());
            map.put(COLUMN_SOURCE_HOST, sqlRecord.getSourceHost());
            map.put(COLUMN_SOURCE_PORT, sqlRecord.getSourcePort() + "");
            map.put(COLUMN_ROWS, sqlRecord.getRows() + "");
            map.put(COLUMN_EXAMINED_ROWS, sqlRecord.getExaminedRows() + "");
            map.put(COLUMN_DURATION, sqlRecord.getDuration() + "");
            map.put(COLUMN_START_TIME, FORMATTER.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(sqlRecord.getStartTime()), ZoneId.systemDefault())));
            list.add(map);
        }));
        return list;
    }
}
