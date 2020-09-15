package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Maps;

import java.util.*;

public class DbleEntryDbGroup extends ManagerBaseTable {

    private static final String TABLE_NAME = "dble_entry_db_group";

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_DB_GROUP = "db_group";

    public DbleEntryDbGroup() {
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
        Set<String> dbGroups = DbleServer.getInstance().getConfig().getDbGroups().keySet();
        DbleServer.getInstance().getConfig().getUsers().entrySet().
                stream().
                sorted((a, b) -> Integer.valueOf(a.getValue().getId()).compareTo(b.getValue().getId())).
                forEach(v -> {
                    UserConfig userConfig = v.getValue();
                    if (userConfig instanceof RwSplitUserConfig) {
                        String dbGroupName = ((RwSplitUserConfig) userConfig).getDbGroup();
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
