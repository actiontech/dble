package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;

import java.util.LinkedHashMap;
import java.util.List;

public class SessionVariables extends ManagerBaseTable {

    private static final String TABLE_NAME = "session_variables";

    private static final String COLUMN_FRONT_ID = "session_conn_id";
    private static final String COLUMN_VAR_NAME = "variable_name";
    private static final String COLUMN_VAR_VALUE = "variable_value";
    private static final String COLUMN_VAR_TYPE = "variable_type";
    private static final String COLUMN_COMMENT = "comment";

    public SessionVariables() {
        super(TABLE_NAME, 5);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_FRONT_ID, new ColumnMeta(COLUMN_FRONT_ID, "int(11)", false));
        columnsType.put(COLUMN_FRONT_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_VAR_NAME, new ColumnMeta(COLUMN_VAR_NAME, "varchar(12)", false));
        columnsType.put(COLUMN_VAR_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_VAR_VALUE, new ColumnMeta(COLUMN_VAR_VALUE, "varchar(12)", false));
        columnsType.put(COLUMN_VAR_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_VAR_TYPE, new ColumnMeta(COLUMN_VAR_TYPE, "varchar(3)", false));
        columnsType.put(COLUMN_VAR_TYPE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_COMMENT, new ColumnMeta(COLUMN_COMMENT, "varchar(1024)", false));
        columnsType.put(COLUMN_COMMENT, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        //        List<LinkedHashMap<String, String>> rows = new ArrayList<>(20);
        //        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
        //            p.getFrontends().
        //                    values().
        //                    forEach(fc -> {
        //                        LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
        //
        //                        row.put(COLUMN_FRONT_ID, fc.getId() + "");
        //                        row.put(COLUMN_VAR_NAME, "");
        //                        row.put(COLUMN_VAR_VALUE, "");
        //                        row.put(COLUMN_VAR_TYPE, "");
        //                        row.put(COLUMN_COMMENT, "");
        //
        //                    });
        //        }

        return null;
    }
}
