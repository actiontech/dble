/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public final class DemoTest2 extends ManagerBaseTable {
    public DemoTest2() {
        super("demotest2", 2);
        initTestData();
    }

    private List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);

    @Override
    protected void initColumnAndType() {
        columns.put("id", new ColumnMeta("id", "int(11)", false, true));
        columnsType.put("id", Fields.FIELD_TYPE_LONG);

        columns.put("name2", new ColumnMeta("name2", "varchar(64)", true));
        columnsType.put("name2", Fields.FIELD_TYPE_VAR_STRING);
    }

    private void initTestData() {
        LinkedHashMap<String, String> row1 = new LinkedHashMap<>();
        row1.put("id", "2");
        row1.put("name2", "YY");
        lst.add(row1);

        LinkedHashMap<String, String> row2 = new LinkedHashMap<>();
        row2.put("id", "3");
        row2.put("name2", "XX");
        lst.add(row2);

        LinkedHashMap<String, String> row3 = new LinkedHashMap<>();
        row3.put("id", "1");
        row3.put("name2", "ZZ");
        lst.add(row3);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {


        return lst;
    }
}
