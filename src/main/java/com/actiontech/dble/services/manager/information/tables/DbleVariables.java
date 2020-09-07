/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.ParamInfo;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.SystemParams;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public final class DbleVariables extends ManagerBaseTable {
    public DbleVariables() {
        super("dble_variables", 4);
        initReadOnlyData();
    }

    private List<LinkedHashMap<String, String>> readOnlyLst = new ArrayList<>();
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
        List<LinkedHashMap<String, String>> lst = new ArrayList<>();
        lst.add(genRow("version_comment", new String(Versions.VERSION_COMMENT), "version_comment", true));
        lst.add(genRow("isOnline", DbleServer.getInstance().isOnline() + "", "When it is set to offline, COM_PING/COM_HEARTBEAT/SELECT USER()/SELECT CURRENT_USER() will return error", false));
        lst.add(genRow("heap_memory_max", Runtime.getRuntime().maxMemory() + "", "The maximum amount of memory that the virtual machine will attempt to use, measured in bytes", true));
        lst.add(genRow("direct_memory_max", Platform.getMaxDirectMemory() + "", "Max direct memory", true));

        lst.addAll(getVolatileParams());
        lst.addAll(readOnlyLst);
        return lst;
    }



    private LinkedHashMap<String, String> genRow(String name, String value, String comment, boolean readOnly) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        row.put("variable_name", name);
        row.put("variable_value", value);
        row.put("comment", comment);
        row.put("read_only", readOnly ? "true" : "false");
        return row;
    }

    private List<LinkedHashMap<String, String>> getVolatileParams() {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>();
        for (ParamInfo volatileParam : SystemParams.getInstance().getVolatileParams()) {
            lst.add(genRow(volatileParam.getName(), volatileParam.getValue(), volatileParam.getComment(), false));
        }
        return lst;
    }

    private void initReadOnlyData() {
        for (ParamInfo readOnlyParam : SystemParams.getInstance().getReadOnlyParams()) {
            readOnlyLst.add(genRow(readOnlyParam.getName(), readOnlyParam.getValue(), readOnlyParam.getComment(), true));
        }
    }
}
