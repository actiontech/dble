/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.SingleDbGroupUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class OBsharding_DEntryDbGroup extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding-d_entry_db_group";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_DB_GROUP = "db_group";

    public OBsharding_DEntryDbGroup() {
        super(TABLE_NAME, 2);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "int(11)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DB_GROUP, new ColumnMeta(COLUMN_DB_GROUP, "varchar(64)", false));
        columnsType.put(COLUMN_DB_GROUP, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        Set<String> dbGroups = OBsharding_DServer.getInstance().getConfig().getDbGroups().keySet();
        OBsharding_DServer.getInstance().getConfig().getUsers().entrySet().
                stream().
                sorted((a, b) -> Integer.compare(a.getValue().getId(), b.getValue().getId())).
                forEach(v -> {
                    UserConfig userConfig = v.getValue();
                    if (userConfig instanceof SingleDbGroupUserConfig) {
                        String dbGroupName = ((SingleDbGroupUserConfig) userConfig).getDbGroup();
                        if (dbGroups.contains(dbGroupName)) {
                            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                            map.put(COLUMN_ID, userConfig.getId() + "");
                            map.put(COLUMN_DB_GROUP, dbGroupName);
                            list.add(map);
                        }
                    }
                });
        return list;
    }
}
