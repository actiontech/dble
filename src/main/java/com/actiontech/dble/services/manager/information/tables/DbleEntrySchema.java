package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.*;

public class DbleEntrySchema extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_entry_schema";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_SCHEMA = "schema";

    public DbleEntrySchema() {
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
        DbleServer.getInstance().getConfig().getUsers().entrySet().
                stream().
                sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).
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
