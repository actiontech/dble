/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.user.ShardingUserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.*;

public class OBsharding_DEntrySchema extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding-d_entry_schema";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_SCHEMA = "schema";

    public OBsharding_DEntrySchema() {
        super(TABLE_NAME, 2);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_ID, new ColumnMeta(COLUMN_ID, "int(11)", false, true));
        columnsType.put(COLUMN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_SCHEMA, new ColumnMeta(COLUMN_SCHEMA, "varchar(64)", false, true));
        columnsType.put(COLUMN_SCHEMA, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        OBsharding_DServer.getInstance().getConfig().getUsers().entrySet().
                stream().
                sorted((a, b) -> Integer.compare(a.getValue().getId(), b.getValue().getId())).
                forEach(v -> {
                    UserConfig userConfig = v.getValue();
                    if (userConfig instanceof ShardingUserConfig) {
                        for (String schema : ((ShardingUserConfig) userConfig).getSchemas()) {
                            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
                            map.put(COLUMN_ID, userConfig.getId() + "");
                            map.put(COLUMN_SCHEMA, schema);
                            list.add(map);
                        }
                    }
                });
        return list;
    }
}
