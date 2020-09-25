/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class DbleDbGroup extends ManagerWritableTable {

    public static final String TABLE_NAME = "dble_db_group";

    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_HEARTBEAT_STMT = "heartbeat_stmt";
    public static final String COLUMN_HEARTBEAT_TIMEOUT = "heartbeat_timeout";
    public static final String COLUMN_HEARTBEAT_RETRY = "heartbeat_retry";
    public static final String COLUMN_RW_SPLIT_MODE = "rw_split_mode";
    public static final String COLUMN_DELAY_THRESHOLD = "delay_threshold";
    public static final String COLUMN_DISABLE_HA = "disable_ha";
    public static final String COLUMN_ACTIVE = "active";

    private final List<LinkedHashMap<String, String>> tempRowList = Lists.newArrayList();

    public DbleDbGroup() {
        super(TABLE_NAME, 8);
        setNotWritableColumnSet(COLUMN_ACTIVE);
        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator + ConfigFileName.DB_XML;
        this.setXmlFilePath(path);
    }

    @Override
    protected void initColumnAndType() {

        columns.put(COLUMN_NAME, new ColumnMeta(COLUMN_NAME, "varchar(64)", false, true));
        columnsType.put(COLUMN_NAME, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HEARTBEAT_STMT, new ColumnMeta(COLUMN_HEARTBEAT_STMT, "varchar(64)", false));
        columnsType.put(COLUMN_HEARTBEAT_STMT, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_HEARTBEAT_TIMEOUT, new ColumnMeta(COLUMN_HEARTBEAT_TIMEOUT, "int(11)", true, "0"));
        columnsType.put(COLUMN_HEARTBEAT_TIMEOUT, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_HEARTBEAT_RETRY, new ColumnMeta(COLUMN_HEARTBEAT_RETRY, "int(11)", true, "1"));
        columnsType.put(COLUMN_HEARTBEAT_RETRY, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_RW_SPLIT_MODE, new ColumnMeta(COLUMN_RW_SPLIT_MODE, "int(11)", false));
        columnsType.put(COLUMN_RW_SPLIT_MODE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DELAY_THRESHOLD, new ColumnMeta(COLUMN_DELAY_THRESHOLD, "int(11)", true, "-1"));
        columnsType.put(COLUMN_DELAY_THRESHOLD, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DISABLE_HA, new ColumnMeta(COLUMN_DISABLE_HA, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_DISABLE_HA, Fields.FIELD_TYPE_VAR_STRING);

        columns.put(COLUMN_ACTIVE, new ColumnMeta(COLUMN_ACTIVE, "varchar(5)", true, "false"));
        columnsType.put(COLUMN_ACTIVE, Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        Map<String, PhysicalDbGroup> dbGroupMap = DbleServer.getInstance().getConfig().getDbGroups();
        List<LinkedHashMap<String, String>> rowList = dbGroupMap.values().stream().map(dbGroup -> {
            DbGroupConfig dbGroupConfig = dbGroup.getDbGroupConfig();
            LinkedHashMap<String, String> map = initMap(dbGroupConfig);
            map.put(COLUMN_ACTIVE, Boolean.TRUE.toString());
            return map;
        }).collect(Collectors.toList());
        rowList.addAll(tempRowList);
        return rowList;
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) throws SQLException {
        //check rule
        rows.forEach(this::checkRule);
        tempRowList.addAll(rows);
        this.setMsg("The above row is temporarily stored, please insert the relevant db_instance.");
        return rows.size();
    }

    @Override
    public int updateRows(Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws SQLException {
        checkRule(values);
        //temp
        List<LinkedHashMap<String, String>> dbGroupRows = Lists.newArrayList();
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            boolean isTemp = false;
            for (LinkedHashMap<String, String> tempRow : tempRowList) {
                if (StringUtil.equals(tempRow.get(COLUMN_NAME), affectPk.get(COLUMN_NAME))) {
                    isTemp = true;
                    tempRow.putAll(values);
                }
            }
            if (!isTemp) {
                affectPk.putAll(values);
                dbGroupRows.add(affectPk);
            }
        }
        if (dbGroupRows.isEmpty()) {
            return affectPks.size();
        }
        XmlProcessBase xmlProcess = new XmlProcessBase();
        DbleDbInstance dbleDbInstance = (DbleDbInstance) ManagerSchemaInfo.getInstance().getTables().get(DbleDbInstance.TABLE_NAME);
        DbGroups dbs = dbleDbInstance.transformRow(xmlProcess, dbGroupRows, null);

        dbs.encryptPassword();
        xmlProcess.writeObjToXml(dbs, getXmlFilePath(), "db");
        return affectPks.size();
    }

    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) throws SQLException {
        //check
        checkDeleteRule(affectPks);
        //temp
        List<LinkedHashMap<String, String>> dbGroupRows = Lists.newArrayList();
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            boolean isTemp = tempRowList.removeIf(tempRow -> StringUtil.equals(tempRow.get(COLUMN_NAME), affectPk.get(COLUMN_NAME)));
            if (!isTemp) {
                dbGroupRows.add(affectPk);
            }
        }
        if (dbGroupRows.isEmpty()) {
            return affectPks.size();
        }

        XmlProcessBase xmlProcess = new XmlProcessBase();
        DbleDbInstance dbleDbInstance = (DbleDbInstance) ManagerSchemaInfo.getInstance().getTables().get(DbleDbInstance.TABLE_NAME);
        DbGroups dbs = dbleDbInstance.transformRow(xmlProcess, null, null);
        for (LinkedHashMap<String, String> affectPk : dbGroupRows) {
            dbs.getDbGroup().removeIf(dbGroup -> StringUtil.equals(dbGroup.getName(), affectPk.get(COLUMN_NAME)));
        }

        dbs.encryptPassword();
        xmlProcess.writeObjToXml(dbs, getXmlFilePath(), "db");
        return affectPks.size();
    }

    private void checkDeleteRule(Set<LinkedHashMap<String, String>> affectPks) {
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            //check user-group
            DbleRwSplitEntry dbleRwSplitEntry = (DbleRwSplitEntry) ManagerSchemaInfo.getInstance().getTables().get(DbleRwSplitEntry.TABLE_NAME);
            boolean existUser = dbleRwSplitEntry.getRows().stream().anyMatch(entry -> entry.get(DbleRwSplitEntry.COLUMN_DB_GROUP).equals(affectPk.get(COLUMN_NAME)));
            if (existUser) {
                throw new ConfigException("Cannot delete or update a parent row: a foreign key constraint fails `dble_db_user`(`db_group`) REFERENCES `dble_db_group`(`name`)");
            }
            //check instance-group
            DbleDbInstance dbleDbInstance = (DbleDbInstance) ManagerSchemaInfo.getInstance().getTables().get(DbleDbInstance.TABLE_NAME);
            boolean existInstance = dbleDbInstance.getRows().stream().anyMatch(entry -> entry.get(DbleDbInstance.COLUMN_DB_GROUP).equals(affectPk.get(COLUMN_NAME)));
            if (existInstance) {
                throw new ConfigException("Cannot delete or update a parent row: a foreign key constraint fails `dble_db_instance`(`db_group`) REFERENCES `dble_db_group`(`name`)");
            }
            //check sharding_node-group
            DbleShardingNode dbleShardingNode = (DbleShardingNode) ManagerSchemaInfo.getInstance().getTables().get(DbleShardingNode.TABLE_NAME);
            boolean existShardingNode = dbleShardingNode.getRows().stream().anyMatch(entry -> entry.get(DbleShardingNode.COLUMN_DB_GROUP).equals(affectPk.get(COLUMN_NAME)));
            if (existShardingNode) {
                throw new ConfigException("Cannot delete or update a parent row: a foreign key constraint fails `dble_sharding_node`(`db_group`) REFERENCES `dble_db_group`(`name`)");
            }
        }
    }

    public List<LinkedHashMap<String, String>> getTempRowList() {
        return tempRowList;
    }

    private void checkRule(LinkedHashMap<String, String> row) {
        if (null != row && !row.isEmpty()) {
            String delayThresholdStr = row.get(COLUMN_DELAY_THRESHOLD);
            int delayThreshold = StringUtil.isEmpty(delayThresholdStr) ? 0 : Integer.parseInt(delayThresholdStr);
            String disableHaStr = row.get(COLUMN_DISABLE_HA);
            if (!StringUtil.isEmpty(disableHaStr) && !StringUtil.equalsIgnoreCase(disableHaStr, Boolean.FALSE.toString()) &&
                    !StringUtil.equalsIgnoreCase(disableHaStr, Boolean.TRUE.toString())) {
                throw new ConfigException("Column 'disable_ha' values only support 'false' or 'true'.");
            }
            boolean disableHa = !StringUtil.isEmpty(disableHaStr) && Boolean.parseBoolean(disableHaStr);
            String rwSplitModeStr = row.get(COLUMN_RW_SPLIT_MODE);
            int rwSplitMode = StringUtil.isEmpty(rwSplitModeStr) ? 0 : Integer.parseInt(rwSplitModeStr);
            String heartbeatTimeoutStr = row.get(COLUMN_HEARTBEAT_TIMEOUT);
            int heartbeatTimeout = StringUtil.isEmpty(heartbeatTimeoutStr) ? 0 : Integer.parseInt(heartbeatTimeoutStr);
            String heartbeatRetryStr = row.get(COLUMN_HEARTBEAT_RETRY);
            int heartbeatRetry = StringUtil.isEmpty(heartbeatRetryStr) ? 0 : Integer.parseInt(heartbeatRetryStr);
            DbGroupConfig dbGroupConfig = new DbGroupConfig(row.get(COLUMN_NAME), null, null, delayThreshold, disableHa);
            dbGroupConfig.setRwSplitMode(rwSplitMode);
            dbGroupConfig.setHeartbeatSQL(row.get(COLUMN_HEARTBEAT_STMT));
            dbGroupConfig.setHeartbeatTimeout(heartbeatTimeout * 1000);
            dbGroupConfig.setErrorRetryCount(heartbeatRetry);

            LinkedHashMap<String, String> map = initMap(dbGroupConfig);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (row.containsKey(entry.getKey())) {
                    row.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private LinkedHashMap<String, String> initMap(DbGroupConfig dbGroupConfig) {
        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
        map.put(COLUMN_NAME, dbGroupConfig.getName());
        map.put(COLUMN_HEARTBEAT_STMT, dbGroupConfig.getHeartbeatSQL());
        map.put(COLUMN_HEARTBEAT_TIMEOUT, String.valueOf(dbGroupConfig.getHeartbeatTimeout() / 1000));
        map.put(COLUMN_HEARTBEAT_RETRY, String.valueOf(dbGroupConfig.getErrorRetryCount()));
        map.put(COLUMN_RW_SPLIT_MODE, String.valueOf(dbGroupConfig.getRwSplitMode()));
        map.put(COLUMN_DELAY_THRESHOLD, String.valueOf(dbGroupConfig.getDelayThreshold()));
        map.put(COLUMN_DISABLE_HA, String.valueOf(dbGroupConfig.isDisableHA()));
        return map;
    }
}
