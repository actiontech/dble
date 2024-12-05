/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.XAAnalysisHandler;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OBsharding_DXaRecover extends ManagerBaseTable {

    private static final String TABLE_NAME = "obsharding_d_xa_recover";

    private static final String COLUMN_DBGROUP = "dbgroup";
    private static final String COLUMN_INSTANCE = "instance";
    private static final String COLUMN_IP = "ip";
    private static final String COLUMN_PORT = "port";
    private static final String COLUMN_FORMATID = "formatid";
    private static final String COLUMN_GTRID_LENGTH = "gtrid_length";
    private static final String COLUMN_BQUAL_LENGTH = "bqual_length";
    private static final String COLUMN_DATA = "data";

    public OBsharding_DXaRecover() {
        super(TABLE_NAME, 8);
    }

    @Override
    protected void initColumnAndType() {
        columns.put(COLUMN_DBGROUP, new ColumnMeta(COLUMN_DBGROUP, "varchar(20)", false, true));
        columnsType.put(COLUMN_DBGROUP, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_INSTANCE, new ColumnMeta(COLUMN_INSTANCE, "varchar(20)", false, true));
        columnsType.put(COLUMN_INSTANCE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_IP, new ColumnMeta(COLUMN_IP, "varchar(20)", false, true));
        columnsType.put(COLUMN_IP, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_PORT, new ColumnMeta(COLUMN_PORT, "int(5)", false));
        columnsType.put(COLUMN_PORT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_FORMATID, new ColumnMeta(COLUMN_FORMATID, "int(11)", false));
        columnsType.put(COLUMN_FORMATID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_GTRID_LENGTH, new ColumnMeta(COLUMN_GTRID_LENGTH, "int(11)", false));
        columnsType.put(COLUMN_GTRID_LENGTH, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_BQUAL_LENGTH, new ColumnMeta(COLUMN_BQUAL_LENGTH, "int(11)", false));
        columnsType.put(COLUMN_BQUAL_LENGTH, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DATA, new ColumnMeta(COLUMN_DATA, "varchar(20)", false));
        columnsType.put(COLUMN_DATA, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> rowList = Lists.newLinkedList();
        XAAnalysisHandler xaAnalysisHandler = new XAAnalysisHandler();
        Map<PhysicalDbInstance, List<Map<String, String>>> recoverMap = xaAnalysisHandler.select();
        for (Map.Entry<PhysicalDbInstance, List<Map<String, String>>> rm : recoverMap.entrySet()) {
            if (rm.getValue() == null)
                continue;
            for (Map<String, String> recover : rm.getValue()) {
                LinkedHashMap<String, String> row = Maps.newLinkedHashMap();
                row.put(COLUMN_DBGROUP, rm.getKey().getDbGroup().getGroupName());
                row.put(COLUMN_INSTANCE, rm.getKey().getConfig().getInstanceName());
                row.put(COLUMN_IP, rm.getKey().getConfig().getIp());
                row.put(COLUMN_PORT, String.valueOf(rm.getKey().getConfig().getPort()));
                row.put(COLUMN_FORMATID, recover.get("formatID"));
                row.put(COLUMN_GTRID_LENGTH, recover.get(COLUMN_GTRID_LENGTH));
                row.put(COLUMN_BQUAL_LENGTH, recover.get(COLUMN_BQUAL_LENGTH));
                row.put(COLUMN_DATA, recover.get(COLUMN_DATA));
                rowList.add(row);
            }
        }
        return rowList;
    }
}
