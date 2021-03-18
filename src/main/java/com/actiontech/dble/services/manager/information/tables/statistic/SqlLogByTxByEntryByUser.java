package com.actiontech.dble.services.manager.information.tables.statistic;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.sql.handler.SqlStatisticHandler;
import com.actiontech.dble.statistic.sql.handler.StatisticDataHandler;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class SqlLogByTxByEntryByUser extends ManagerBaseTable {
    public static final String TABLE_NAME = "sql_log_by_tx_by_entry_by_user";

    private static final String COLUMN_TX_ID = "tx_id";
    private static final String COLUMN_ENTRY = "entry";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_SOURCE_HOST = "source_host";
    private static final String COLUMN_SOURCE_PORT = "source_port";
    private static final String COLUMN_SQL_IDS = "sql_ids";
    private static final String COLUMN_SQL_COUNT = "sql_count";
    private static final String COLUMN_TX_DURATION = "tx_duration";
    private static final String COLUMN_BUSY_TIME = "busy_time";
    private static final String COLUMN_EXAMINED_ROWS = "examined_rows";


    public SqlLogByTxByEntryByUser() {
        super(TABLE_NAME, 10);
        useTruncate();
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_TX_ID, new ColumnMeta(COLUMN_TX_ID, "int(11)", false, true));
        columnsType.put(COLUMN_TX_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_ENTRY, new ColumnMeta(COLUMN_ENTRY, "int(11)", false, true));
        columnsType.put(COLUMN_ENTRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(20)", false, true));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SOURCE_HOST, new ColumnMeta(COLUMN_SOURCE_HOST, "varchar(20)", false));
        columnsType.put(COLUMN_SOURCE_HOST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SOURCE_PORT, new ColumnMeta(COLUMN_SOURCE_PORT, "int(11)", false));
        columnsType.put(COLUMN_SOURCE_PORT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_IDS, new ColumnMeta(COLUMN_SQL_IDS, "varchar(1024)", false));
        columnsType.put(COLUMN_SQL_IDS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_COUNT, new ColumnMeta(COLUMN_SQL_COUNT, "int(11)", false));
        columnsType.put(COLUMN_SQL_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_TX_DURATION, new ColumnMeta(COLUMN_TX_DURATION, "int(11)", false));
        columnsType.put(COLUMN_TX_DURATION, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_BUSY_TIME, new ColumnMeta(COLUMN_BUSY_TIME, "int(11)", false));
        columnsType.put(COLUMN_BUSY_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_EXAMINED_ROWS, new ColumnMeta(COLUMN_EXAMINED_ROWS, "int(11)", false));
        columnsType.put(COLUMN_EXAMINED_ROWS, Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        StatisticDataHandler dataHandler = StatisticManager.getInstance().getHandler(SqlLog.TABLE_NAME);
        if (dataHandler == null) {
            return list;
        }

        LinkedList<SqlStatisticHandler.TxRecord> txs = (LinkedList<SqlStatisticHandler.TxRecord>) dataHandler.getList();
        txs.forEach(txRecord -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_TX_ID, txRecord.getTxId() + "");
            map.put(COLUMN_ENTRY, txRecord.getInfo().getUserId() + "");
            map.put(COLUMN_USER, txRecord.getInfo().getUser());
            map.put(COLUMN_SOURCE_HOST, txRecord.getInfo().getHost());
            map.put(COLUMN_SOURCE_PORT, txRecord.getInfo().getPort() + "");

            long examinedRows = 0;
            long busyTime = 0;
            StringBuilder sqlIds = new StringBuilder(50);
            for (SqlStatisticHandler.SQLRecord sqlRecord : txRecord.getSqls()) {
                examinedRows += sqlRecord.getExaminedRows();
                busyTime += sqlRecord.getDuration();
                sqlIds.append(sqlRecord.getSqlId()).append(",");
            }

            map.put(COLUMN_SQL_IDS, sqlIds.deleteCharAt(sqlIds.length() - 1).toString());
            map.put(COLUMN_SQL_COUNT, txRecord.getSqls().size() + "");
            map.put(COLUMN_EXAMINED_ROWS, examinedRows + "");
            map.put(COLUMN_TX_DURATION, txRecord.getDuration() + "");
            map.put(COLUMN_BUSY_TIME, busyTime + "");
            list.add(map);
        });
        return list;
    }

}
