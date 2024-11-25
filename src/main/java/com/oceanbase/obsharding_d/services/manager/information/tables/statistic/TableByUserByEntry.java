/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables.statistic;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.statistic.sql.handler.StatisticDataHandler;
import com.oceanbase.obsharding_d.statistic.sql.handler.TableByUserByEntryCalcHandler;
import com.oceanbase.obsharding_d.util.DateUtil;
import com.google.common.collect.Maps;

import java.util.*;

public class TableByUserByEntry extends ManagerBaseTable {

    public static final String TABLE_NAME = "sql_statistic_by_table_by_user_by_entry";
    private static final String COLUMN_ENTRY = "entry";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_TABLE = "table";
    private static final String COLUMN_SQL_INSERT_COUNT = "sql_insert_count";
    private static final String COLUMN_SQL_INSERT_ROWS = "sql_insert_rows";
    private static final String COLUMN_SQL_INSERT_TIME = "sql_insert_time";
    private static final String COLUMN_SQL_UPDATE_COUNT = "sql_update_count";
    private static final String COLUMN_SQL_UPDATE_ROWS = "sql_update_rows";
    private static final String COLUMN_SQL_UPDATE_TIME = "sql_update_time";
    private static final String COLUMN_SQL_DELETE_COUNT = "sql_delete_count";
    private static final String COLUMN_SQL_DELETE_ROWS = "sql_delete_rows";
    private static final String COLUMN_SQL_DELETE_TIME = "sql_delete_time";
    private static final String COLUMN_SQL_SELECT_COUNT = "sql_select_count";
    private static final String COLUMN_SQL_SELECT_EXAMINED_ROWS = "sql_select_examined_rows";
    private static final String COLUMN_SQL_SELECT_ROWS = "sql_select_rows";
    private static final String COLUMN_SQL_SELECT_TIME = "sql_select_time";
    private static final String COLUMN_LAST_UPDATE_TIME = "last_update_time";

    public TableByUserByEntry() {
        super(TABLE_NAME, 17);
        useTruncate();
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ENTRY, new ColumnMeta(COLUMN_ENTRY, "int(11)", false, true));
        columnsType.put(COLUMN_ENTRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(20)", false, true));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_TABLE, new ColumnMeta(COLUMN_TABLE, "varchar(20)", false, true));
        columnsType.put(COLUMN_TABLE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SQL_INSERT_COUNT, new ColumnMeta(COLUMN_SQL_INSERT_COUNT, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_INSERT_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_INSERT_ROWS, new ColumnMeta(COLUMN_SQL_INSERT_ROWS, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_INSERT_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_INSERT_TIME, new ColumnMeta(COLUMN_SQL_INSERT_TIME, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_INSERT_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_UPDATE_COUNT, new ColumnMeta(COLUMN_SQL_UPDATE_COUNT, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_UPDATE_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_UPDATE_ROWS, new ColumnMeta(COLUMN_SQL_UPDATE_ROWS, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_UPDATE_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_UPDATE_TIME, new ColumnMeta(COLUMN_SQL_UPDATE_TIME, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_UPDATE_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_DELETE_COUNT, new ColumnMeta(COLUMN_SQL_DELETE_COUNT, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_DELETE_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_DELETE_ROWS, new ColumnMeta(COLUMN_SQL_DELETE_ROWS, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_DELETE_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_DELETE_TIME, new ColumnMeta(COLUMN_SQL_DELETE_TIME, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_DELETE_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_SELECT_COUNT, new ColumnMeta(COLUMN_SQL_SELECT_COUNT, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_SELECT_COUNT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_SELECT_ROWS, new ColumnMeta(COLUMN_SQL_SELECT_ROWS, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_SELECT_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_SELECT_EXAMINED_ROWS, new ColumnMeta(COLUMN_SQL_SELECT_EXAMINED_ROWS, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_SELECT_EXAMINED_ROWS, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SQL_SELECT_TIME, new ColumnMeta(COLUMN_SQL_SELECT_TIME, "int(11)", false, false));
        columnsType.put(COLUMN_SQL_SELECT_TIME, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_LAST_UPDATE_TIME, new ColumnMeta(COLUMN_LAST_UPDATE_TIME, "varchar(26)", false, false));
        columnsType.put(COLUMN_LAST_UPDATE_TIME, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        StatisticDataHandler dataHandler = StatisticManager.getInstance().getHandler(TABLE_NAME);
        if (dataHandler == null) {
            return list;
        }
        Map<String, TableByUserByEntryCalcHandler.Record> recordMap = (Map<String, TableByUserByEntryCalcHandler.Record>) dataHandler.getList();
        recordMap.entrySet().stream().sorted(Comparator.comparingInt(a -> a.getValue().getEntry())).forEach(v -> {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_ENTRY, String.valueOf(v.getValue().getEntry()));
            map.put(COLUMN_USER, v.getValue().getUser());
            map.put(COLUMN_TABLE, v.getValue().getTable());

            map.put(COLUMN_SQL_INSERT_COUNT, String.valueOf(v.getValue().getInsertCount()));
            map.put(COLUMN_SQL_INSERT_ROWS, String.valueOf(v.getValue().getInsertRows()));
            map.put(COLUMN_SQL_INSERT_TIME, String.valueOf(v.getValue().getInsertTime() / 1000));

            map.put(COLUMN_SQL_UPDATE_COUNT, String.valueOf(v.getValue().getUpdateCount()));
            map.put(COLUMN_SQL_UPDATE_ROWS, String.valueOf(v.getValue().getUpdateRows()));
            map.put(COLUMN_SQL_UPDATE_TIME, String.valueOf(v.getValue().getUpdateTime() / 1000));

            map.put(COLUMN_SQL_DELETE_COUNT, String.valueOf(v.getValue().getDeleteCount()));
            map.put(COLUMN_SQL_DELETE_ROWS, String.valueOf(v.getValue().getDeleteRows()));
            map.put(COLUMN_SQL_DELETE_TIME, String.valueOf(v.getValue().getDeleteTime() / 1000));

            map.put(COLUMN_SQL_SELECT_EXAMINED_ROWS, String.valueOf(v.getValue().getSelectExaminedRowsRows()));
            map.put(COLUMN_SQL_SELECT_COUNT, String.valueOf(v.getValue().getSelectCount()));
            map.put(COLUMN_SQL_SELECT_ROWS, String.valueOf(v.getValue().getSelectRows()));
            map.put(COLUMN_SQL_SELECT_TIME, String.valueOf(v.getValue().getSelectTime() / 1000));

            map.put(COLUMN_LAST_UPDATE_TIME, DateUtil.parseStr(v.getValue().getLastUpdateTime()));
            list.add(map);
        });
        return list;
    }
}
