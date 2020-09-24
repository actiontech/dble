/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.util.NameableExecutor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public final class DbleThreadPool extends ManagerBaseTable {
    public DbleThreadPool() {
        super("dble_thread_pool", 4);
    }

    @Override
    protected void initColumnAndType() {

        columns.put("name", new ColumnMeta("name", "varchar(32)", false, true));
        columnsType.put("name", Fields.FIELD_TYPE_VAR_STRING);

        columns.put("size", new ColumnMeta("size", "int(11)", false));
        columnsType.put("size", Fields.FIELD_TYPE_LONG);

        columns.put("active_count", new ColumnMeta("active_count", "int(11)", false));
        columnsType.put("active_count", Fields.FIELD_TYPE_LONG);

        columns.put("waiting_task_count", new ColumnMeta("waiting_task_count", "int(11)", false));
        columnsType.put("waiting_task_count", Fields.FIELD_TYPE_LONG);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        DbleServer server = DbleServer.getInstance();
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);
        lst.add(getRow((NameableExecutor) server.getTimerExecutor()));
        lst.add(getRow((NameableExecutor) server.getBusinessExecutor()));
        lst.add(getRow((NameableExecutor) server.getBackendBusinessExecutor()));
        lst.add(getRow((NameableExecutor) server.getComplexQueryExecutor()));
        lst.add(getRow((NameableExecutor) server.getWriteToBackendExecutor()));
        return lst;
    }

    private LinkedHashMap<String, String> getRow(NameableExecutor exec) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        row.put("name", exec.getName());
        row.put("size", exec.getPoolSize() + "");
        row.put("active_count", exec.getActiveCount() + "");
        row.put("waiting_task_count", exec.getQueue().size() + "");
        return row;
    }

}
