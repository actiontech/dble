/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.Versions;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public final class Version extends ManagerBaseTable {
    public Version() {
        super("version", 1);
    }

    @Override
    protected void initColumnAndType() {

        columns.put("version", new ColumnMeta("version", "varchar(64)", false, true));
        columnsType.put("version", Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> lst = new ArrayList<>(1);
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        row.put("version", new String(Versions.getServerVersion()));
        lst.add(row);

        return lst;
    }
}
