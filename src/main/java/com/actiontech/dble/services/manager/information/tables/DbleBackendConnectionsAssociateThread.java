/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.singleton.ConnectionAssociateThreadManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;

public class DbleBackendConnectionsAssociateThread extends ManagerBaseTable {

    public DbleBackendConnectionsAssociateThread() {
        super("backend_connections_associate_thread", 2);
    }

    @Override
    protected void initColumnAndType() {
        columns.put("backend_conn_id", new ColumnMeta("backend_conn_id", "int(11)", false, true));
        columnsType.put("backend_conn_id", Fields.FIELD_TYPE_LONG);

        columns.put("thread_name", new ColumnMeta("thread_name", "varchar(64)", false));
        columnsType.put("thread_name", Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<ConnectionAssociateThreadManager.AssociateVector> list = ConnectionAssociateThreadManager.getInstance().getVector(ConnectionAssociateThreadManager.ConnectType.Backend);
        List<LinkedHashMap<String, String>> result = Lists.newArrayList();
        list.stream().
                sorted(Comparator.comparingLong(c -> c.getConnId())).
                forEach(c -> {
                    LinkedHashMap<String, String> mapi = Maps.newLinkedHashMap();
                    mapi.put("backend_conn_id", c.getConnId() + "");
                    mapi.put("thread_name", c.getThreadName());
                    result.add(mapi);
                });
        return result;
    }
}
