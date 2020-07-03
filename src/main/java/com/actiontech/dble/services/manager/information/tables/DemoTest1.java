/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;

import java.sql.SQLException;
import java.util.*;

public final class DemoTest1 extends ManagerWritableTable {
    public DemoTest1() {
        super("demotest1", 2);
        initTestData();
    }

    private List<LinkedHashMap<String, String>> lst = new ArrayList<>(5);

    @Override
    protected void initColumnAndType() {
        columns.put("id", new ColumnMeta("id", "int(11)", false, true));
        columnsType.put("id", Fields.FIELD_TYPE_LONG);

        columns.put("name1", new ColumnMeta("name1", "varchar(64)", false));
        columnsType.put("name1", Fields.FIELD_TYPE_VAR_STRING);
    }

    private void initTestData() {
        LinkedHashMap<String, String> row1 = new LinkedHashMap<>();
        row1.put("id", "2");
        row1.put("name1", "YY");
        lst.add(row1);

        LinkedHashMap<String, String> row2 = new LinkedHashMap<>();
        row2.put("id", "3");
        row2.put("name1", "XX");
        lst.add(row2);

        LinkedHashMap<String, String> row3 = new LinkedHashMap<>();
        row3.put("id", "1");
        row3.put("name1", "ZZ");
        lst.add(row3);

        LinkedHashMap<String, String> row4 = new LinkedHashMap<>();
        row4.put("id", "-1");
        row4.put("name1", "aa");
        lst.add(row4);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        return lst;
    }

    @Override
    public void insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        lst.addAll(rows);
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, Map<String, String> values) throws SQLException {
        for (LinkedHashMap<String, String> row : lst) {
            LinkedHashMap<String, String> pk = new LinkedHashMap<>(getPrimaryKeyColumns().size());
            for (String pkColumn : getPrimaryKeyColumns()) {
                pk.put(pkColumn, row.get(pkColumn));
            }
            if (affectPks.contains(pk)) {
                for (Map.Entry<String, String> value : values.entrySet()) {
                    row.put(value.getKey(), value.getValue());
                }
            }
        }
        return affectPks.size();
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        Iterator<LinkedHashMap<String, String>> iterator = lst.iterator();
        while (iterator.hasNext()) {
            LinkedHashMap<String, String> row = iterator.next();
            LinkedHashMap<String, String> pk = new LinkedHashMap<>(getPrimaryKeyColumns().size());
            for (String pkColumn : getPrimaryKeyColumns()) {
                pk.put(pkColumn, row.get(pkColumn));
            }
            if (affectPks.contains(pk)) {
                iterator.remove();
            }
        }
        return affectPks.size();
    }
}
