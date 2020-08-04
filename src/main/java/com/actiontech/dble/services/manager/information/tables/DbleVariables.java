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

public final class DbleVariables extends ManagerBaseTable {
    public DbleVariables() {
        super("global_variables", 4);
    }

    @Override
    protected void initColumnAndType() {

        columns.put("variable_name", new ColumnMeta("variable_name", "varchar(32)", false, true));
        columnsType.put("variable_name", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("variable_value", new ColumnMeta("variable_value", "varchar(255)", false));
        columnsType.put("variable_value", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("comment", new ColumnMeta("comment", "varchar(255)", false));
        columnsType.put("comment", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("read_only", new ColumnMeta("read_only", "varchar(7)", false));
        columnsType.put("read_only", Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        //todo
        return null;
    }
}
