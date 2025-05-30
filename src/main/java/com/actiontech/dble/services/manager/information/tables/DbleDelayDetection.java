/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.heartbeat.MySQLHeartbeat;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class DbleDelayDetection extends ManagerWritableTable {
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

    public DbleDelayDetection() {
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
        ServerConfig conf = DbleServer.getInstance().getConfig();
        Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
        for (PhysicalDbGroup dbGroup : dbGroups.values()) {
            if (dbGroup.getDbGroupConfig().isDelayDetection()) {
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
        final MySQLHeartbeat heartbeat = dbInstance.getHeartbeat();
        if (Objects.isNull(heartbeat) || heartbeat.isStop()) {
            return row;
        }
        DbInstanceConfig config = dbInstance.getConfig();
        row.put(COLUMN_DB_GROUP_NAME, dbInstance.getDbGroup().getGroupName());
        row.put(COLUMN_NAME, dbInstance.getName());
        row.put(COLUMN_HOST, config.getUrl());
        row.put(COLUMN_DELAY, String.valueOf(heartbeat.getSlaveBehindMaster()));
        row.put(COLUMN_STATUS, String.valueOf(heartbeat.getStatus()));
        row.put(COLUMN_MESSAGE, heartbeat.getMessage());
        row.put(COLUMN_LAST_ACTIVE_TIME, String.valueOf(heartbeat.getLastActiveTime()));
        row.put(COLUMN_LOGIC_UPDATE, String.valueOf(heartbeat.getLogicUpdate()));
        row.put(COLUMN_BACKEND_CONN_ID, String.valueOf(heartbeat.getHeartbeatConnId()));
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
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.writeLock().lock();
        try {
            int val = Integer.parseInt(values.get(COLUMN_LOGIC_UPDATE));
            ServerConfig conf = DbleServer.getInstance().getConfig();
            Map<String, PhysicalDbGroup> dbGroups = conf.getDbGroups();
            List<PhysicalDbInstance> instanceList = Lists.newArrayList();
            for (LinkedHashMap<String, String> affectPk : affectPks) {
                String groupName = affectPk.get(COLUMN_DB_GROUP_NAME);
                String name = affectPk.get(COLUMN_NAME);
                for (PhysicalDbGroup physicalDbGroup : dbGroups.values()) {
                    if (StringUtil.equals(groupName, physicalDbGroup.getGroupName()) && physicalDbGroup.getDbGroupConfig().isDelayDetection()) {
                        PhysicalDbInstance instance = physicalDbGroup.getDbInstances(true).stream().filter(dbInstance -> StringUtil.equals(name, dbInstance.getName()) && Objects.nonNull(dbInstance.getHeartbeat())).findFirst().get();
                        MySQLHeartbeat delayDetection = instance.getHeartbeat();
                        int logicUpdate = delayDetection.getLogicUpdate();
                        if (val != logicUpdate + 1) {
                            throw new SQLException("parameter only increment is allowed to be 1", "42S22", ErrorCode.ER_TABLE_CANT_HANDLE_AUTO_INCREMENT);
                        }
                        instanceList.add(instance);
                    }
                }
            }
            for (PhysicalDbInstance instance : instanceList) {
                instance.getHeartbeat().stop("the management end is shut down manually");
                instance.getHeartbeat().start(instance.getHeartbeatRecoveryTime());
                instance.getHeartbeat().setLogicUpdate(val);
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
