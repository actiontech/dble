/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.singleton.XASessionCheck;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DXaSession extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding_d_xa_session";

    private static final String COLUMN_FRONT_ID = "front_id";
    private static final String COLUMN_XA_ID = "xa_id";
    private static final String COLUMN_XA_STATE = "xa_state";
    private static final String COLUMN_SHARDING_NODE = "sharding_node";

    public OBsharding_DXaSession() {
        super(TABLE_NAME, 4);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_FRONT_ID, new ColumnMeta(COLUMN_FRONT_ID, "int(11)", false, true));
        columnsType.put(COLUMN_FRONT_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_XA_ID, new ColumnMeta(COLUMN_XA_ID, "varchar(20)", false));
        columnsType.put(COLUMN_XA_ID, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_XA_STATE, new ColumnMeta(COLUMN_XA_STATE, "varchar(20)", false));
        columnsType.put(COLUMN_XA_STATE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_SHARDING_NODE, new ColumnMeta(COLUMN_SHARDING_NODE, "varchar(64)", false, true));
        columnsType.put(COLUMN_SHARDING_NODE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> list = new ArrayList<>();
        final XASessionCheck xaCheck = XASessionCheck.getInstance();
        for (NonBlockingSession commitSession : xaCheck.getCommittingSession().values()) {
            getRows(list, commitSession);
        }

        for (NonBlockingSession rollbackSession : xaCheck.getRollbackingSession().values()) {
            getRows(list, rollbackSession);
        }
        return list;
    }

    private void getRows(List<LinkedHashMap<String, String>> list, NonBlockingSession session) {
        for (RouteResultsetNode node : session.getTargetKeys()) {
            LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
            map.put(COLUMN_FRONT_ID, session.getShardingService().getConnection().getId() + "");
            map.put(COLUMN_XA_ID, session.getSessionXaID() + "");
            map.put(COLUMN_XA_STATE, session.getTransactionManager().getXAStage());
            map.put(COLUMN_SHARDING_NODE, node.getName());
            list.add(map);
        }
    }
}
