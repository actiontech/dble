/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables.statistic;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.statistic.sql.handler.AssociateTablesByEntryByUserCalcHandler;
import com.oceanbase.obsharding_d.statistic.sql.handler.StatisticDataHandler;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.util.DateUtil;
import com.google.common.collect.Maps;

import java.util.*;

public class AssociateTablesByEntryByUser extends ManagerBaseTable {

    public static final String TABLE_NAME = "sql_statistic_by_associate_tables_by_entry_by_user";

    private static final String COLUMN_ENTRY = "entry";
    private static final String COLUMN_USER = "user";
    private static final String COLUMN_ASSOCIATE_TABLES = "associate_tables";
    private static final String COLUMN_SQL_SELECT_COUNT = "sql_select_count";
    private static final String COLUMN_SQL_SELECT_EXAMINED_ROWS = "sql_select_examined_rows";
    private static final String COLUMN_SQL_SELECT_ROWS = "sql_select_rows";
    private static final String COLUMN_SQL_SELECT_TIME = "sql_select_time";
    private static final String COLUMN_LAST_UPDATE_TIME = "last_update_time";

    public AssociateTablesByEntryByUser() {
        super(TABLE_NAME, 8);
        useTruncate();
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ENTRY, new ColumnMeta(COLUMN_ENTRY, "int(11)", false, true));
        columnsType.put(COLUMN_ENTRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_USER, new ColumnMeta(COLUMN_USER, "varchar(20)", false, true));
        columnsType.put(COLUMN_USER, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ASSOCIATE_TABLES, new ColumnMeta(COLUMN_ASSOCIATE_TABLES, "varchar(200)", false, true));
        columnsType.put(COLUMN_ASSOCIATE_TABLES, Fields.FIELD_TYPE_VAR_STRING);

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
        Map<String, AssociateTablesByEntryByUserCalcHandler.Record> recordMap = (Map<String, AssociateTablesByEntryByUserCalcHandler.Record>) dataHandler.getList();
        recordMap.entrySet().
                stream().
                sorted(Comparator.comparingInt(a -> a.getValue().getEntry())).
                forEach(v -> {
                    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                    map.put(COLUMN_ENTRY, String.valueOf(v.getValue().getEntry()));
                    map.put(COLUMN_USER, v.getValue().getUser());
                    map.put(COLUMN_ASSOCIATE_TABLES, v.getValue().getTables());
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
