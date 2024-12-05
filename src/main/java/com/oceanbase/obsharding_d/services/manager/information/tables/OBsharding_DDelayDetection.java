/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.backend.delyDetection.DelayDetection;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.db.DbInstanceConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerWritableTable;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class OBsharding_DDelayDetection extends ManagerWritableTable {
    private static final String TABLE_NAME = "delay_detection";

    private static final String COLUMN_DB_GROUP_NAME = "db_group_name";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_HOST = "host";
    private static final String COLUMN_DELAY = "delay";
    private static final String COLUMN_STATUS = "status";
    private static final String COLUMN_MESSAGE = "message";
    private static final String COLUMN_LAST_ACTIVE_TIME = "last_active_time";
    private static final String COLUMN_BACKEND_CONN_ID = "backend_conn_id";
    private static final String COLUMN_LOGIC_UPDATE = "logic_update";

    public OBsharding_DDelayDetection() {
        super(TABLE_NAME, 9);
    }

    @Override
    protected void initColumnAndType() {

        columns.put(COLUMN_DB_GROUP_NAME, new ColumnMeta(COLUMN_DB_GROUP_NAME, "varchar(64)", false));
        columnsType.put(COLUMN_DB_GROUP_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HOST, new ColumnMeta(COLUMN_HOST, "int(11)", false));
        columnsType.put(COLUMN_HOST, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_DELAY, new ColumnMeta(COLUMN_DELAY, "int(11)", false));
        columnsType.put(COLUMN_DELAY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_STATUS, new ColumnMeta(COLUMN_STATUS, "varchar(3)", false));
        columnsType.put(COLUMN_STATUS, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_MESSAGE, new ColumnMeta(COLUMN_MESSAGE, "varchar(1024)", false));
        columnsType.put(COLUMN_MESSAGE, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_LAST_ACTIVE_TIME, new ColumnMeta(COLUMN_LAST_ACTIVE_TIME, "timestamp", false));
        columnsType.put(COLUMN_LAST_ACTIVE_TIME, Fields.FIELD_TYPE_TIMESTAMP);

        columns.put(COLUMN_BACKEND_CONN_ID, new ColumnMeta(COLUMN_BACKEND_CONN_ID, "int(11)", false));
        columnsType.put(COLUMN_BACKEND_CONN_ID, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_LOGIC_UPDATE, new ColumnMeta(COLUMN_LOGIC_UPDATE, "int(11)", false));
        columnsType.put(COLUMN_LOGIC_UPDATE, Fields.FIELD_TYPE_LONG);


    }

    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> results = new ArrayList<>();
        ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
        Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
        for (PhysicalDbGroup dbGroup : dbGroups.values()) {
            if (dbGroup.isDelayDetectionStart()) {
                for (PhysicalDbInstance dbInstance : dbGroup.getDbInstances(true)) {
                    LinkedHashMap<String, String> row = getRow(dbInstance);
                    if (!row.isEmpty()) {
                        results.add(row);
                    }
                }
            }
        }
        return results;
    }

    private LinkedHashMap<String, String> getRow(PhysicalDbInstance dbInstance) {
        LinkedHashMap<String, String> row = new LinkedHashMap<>();
        final DelayDetection delayDetection = dbInstance.getDelayDetection();
        if (Objects.isNull(delayDetection) || delayDetection.isStop()) {
            return row;
        }
        DbInstanceConfig config = dbInstance.getConfig();
        row.put(COLUMN_DB_GROUP_NAME, dbInstance.getDbGroup().getGroupName());
        row.put(COLUMN_NAME, dbInstance.getName());
        row.put(COLUMN_HOST, config.getUrl());
        row.put(COLUMN_DELAY, String.valueOf(delayDetection.getDelayVal()));
        row.put(COLUMN_STATUS, String.valueOf(dbInstance.getDelayDetectionStatus()));
        row.put(COLUMN_MESSAGE, delayDetection.getErrorMessage());
        row.put(COLUMN_LAST_ACTIVE_TIME, String.valueOf(delayDetection.getLastReceivedQryTime()));
        row.put(COLUMN_LOGIC_UPDATE, String.valueOf(delayDetection.getLogicUpdate()));
        Optional.ofNullable(delayDetection.getConn()).ifPresent(connection -> row.put(COLUMN_BACKEND_CONN_ID, String.valueOf(connection.getId())));
        return row;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        throw new SQLException("Access denied for table '" + tableName + "'", "42000", ErrorCode.ER_ACCESS_DENIED_ERROR);
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        if (values.size() != 1 || !values.containsKey(COLUMN_LOGIC_UPDATE)) {
            throw new SQLException("only column '" + COLUMN_LOGIC_UPDATE + "' is writable", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
        }
        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            int val = Integer.parseInt(values.get(COLUMN_LOGIC_UPDATE));
            ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
            Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
            List<PhysicalDbInstance> instanceList = Lists.newArrayList();
            for (LinkedHashMap<String, String> affectPk : affectPks) {
                String groupName = affectPk.get(COLUMN_DB_GROUP_NAME);
                String name = affectPk.get(COLUMN_NAME);
                for (PhysicalDbGroup physicalDbGroup : dbGroups.values()) {
                    if (StringUtil.equals(groupName, physicalDbGroup.getGroupName()) && physicalDbGroup.isDelayDetectionStart()) {
                        PhysicalDbInstance instance = physicalDbGroup.getDbInstances(true).stream().filter(dbInstance -> StringUtil.equals(name, dbInstance.getName()) && Objects.nonNull(dbInstance.getDelayDetection())).findFirst().get();
                        DelayDetection delayDetection = instance.getDelayDetection();
                        int logicUpdate = delayDetection.getLogicUpdate();
                        if (val != logicUpdate + 1) {
                            throw new SQLException("parameter only increment is allowed to be 1", "42S22", ErrorCode.ER_TABLE_CANT_HANDLE_AUTO_INCREMENT);
                        }
                        instanceList.add(instance);
                    }
                }
            }
            for (PhysicalDbInstance instance : instanceList) {
                instance.stopDelayDetection("the management end is shut down manually");
                instance.startDelayDetection();
                instance.getDelayDetection().setLogicUpdate(val);

            }
        } finally {
            lock.writeLock().unlock();
        }
        return affectPks.size();
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        throw new SQLException("Access denied for table '" + tableName + "'", "42000", ErrorCode.ER_ACCESS_DENIED_ERROR);
    }
}
