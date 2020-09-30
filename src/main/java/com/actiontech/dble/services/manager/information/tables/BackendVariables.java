package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.server.variables.VariableType;
import com.actiontech.dble.services.VariablesService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class BackendVariables extends ManagerBaseTable {

    private static final String TABLE_NAME = "backend_variables";

    private static final String COLUMN_BACKED_ID = "backend_conn_id";
    private static final String COLUMN_VAR_NAME = "variable_name";
    private static final String COLUMN_VAR_VALUE = "variable_value";
    private static final String COLUMN_VAR_TYPE = "variable_type";

    public BackendVariables() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_BACKED_ID, new ColumnMeta(COLUMN_BACKED_ID, "int(11)", false));
        columnsType.put(COLUMN_BACKED_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_VAR_NAME, new ColumnMeta(COLUMN_VAR_NAME, "varchar(12)", false));
        columnsType.put(COLUMN_VAR_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_VAR_VALUE, new ColumnMeta(COLUMN_VAR_VALUE, "varchar(12)", false));
        columnsType.put(COLUMN_VAR_VALUE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_VAR_TYPE, new ColumnMeta(COLUMN_VAR_TYPE, "varchar(3)", false));
        columnsType.put(COLUMN_VAR_TYPE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rows = new ArrayList<>(100);
        for (IOProcessor p : DbleServer.getInstance().getBackendProcessors()) {
            p.getBackends().
                    values().
                    forEach(bc -> {
                        AbstractService service = bc.getService();
                        if (service != null) {
                            for (MysqlVariable var : ((VariablesService) service).getAllVars()) {
                                LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
                                row.put(COLUMN_BACKED_ID, bc.getId() + "");
                                row.put(COLUMN_VAR_NAME, var.getName());
                                row.put(COLUMN_VAR_VALUE, var.getValue());
                                row.put(COLUMN_VAR_TYPE, var.getType() == VariableType.SYSTEM_VARIABLES ? "sys" : "user");
                                rows.add(row);
                            }
                        }
                    });
        }

        return rows;
    }
}
