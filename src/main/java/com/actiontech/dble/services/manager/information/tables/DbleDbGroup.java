/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.entity.DbGroups;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.DBGroup;
import com.actiontech.dble.cluster.zkprocess.entity.dbGroups.HeartBeat;
import com.actiontech.dble.config.ConfigFileName;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.converter.DBConverter;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.util.IntegerUtil;
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
    public static final String COLUMN_KEEP_ALIVE = "heartbeat_keep_alive";
    public static final String COLUMN_RW_SPLIT_MODE = "rw_split_mode";
    public static final String COLUMN_DELAY_THRESHOLD = "delay_threshold";
    public static final String COLUMN_DISABLE_HA = "disable_ha";
    public static final String COLUMN_ACTIVE = "active";
    public static final String DELAY_PERIOD_MILLIS = "delay_period_millis";
    public static final String DELAY_DATABASE = "delay_database";

    private final List<LinkedHashMap<String, String>> tempRowList = Lists.newArrayList();

    public DbleDbGroup() {
        super(TABLE_NAME, 9);
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

        columns.put(COLUMN_KEEP_ALIVE, new ColumnMeta(COLUMN_KEEP_ALIVE, "int(11)", true, "60"));
        columnsType.put(COLUMN_KEEP_ALIVE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_RW_SPLIT_MODE, new ColumnMeta(COLUMN_RW_SPLIT_MODE, "int(11)", false));
        columnsType.put(COLUMN_RW_SPLIT_MODE, Fields.FIELD_TYPE_LONG);

        columns.put(COLUMN_DELAY_THRESHOLD, new ColumnMeta(COLUMN_DELAY_THRESHOLD, "int(11)", true, "-1"));
        columnsType.put(COLUMN_DELAY_THRESHOLD, Fields.FIELD_TYPE_LONG);

        columns.put(DELAY_PERIOD_MILLIS, new ColumnMeta(DELAY_PERIOD_MILLIS, "int(11)", true, "-1"));
        columnsType.put(DELAY_PERIOD_MILLIS, Fields.FIELD_TYPE_LONG);

        columns.put(DELAY_DATABASE, new ColumnMeta(DELAY_DATABASE, "varchar(255)", true, null));
        columnsType.put(DELAY_DATABASE, Fields.FIELD_TYPE_VAR_STRING);

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
        tempRowList.forEach(this::defaultVal);
        rowList.addAll(tempRowList);
        return rowList;
    }

    private void defaultVal(LinkedHashMap<String, String> rows) {
        //if the argument is null, then a string equal to "null"
        String delayDatabase = rows.get(DELAY_DATABASE);
        rows.put(DELAY_DATABASE, String.valueOf(delayDatabase));
    }

    @Override
    public int insertRows(List<LinkedHashMap<String, String>> rows) {
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

        List<DBGroup> dbGroupList = affectPks.stream().map(this::transformRowToDBGroup).collect(Collectors.toList());
        DBConverter dbConverter = new DBConverter();
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        DbGroups dbGroups = dbConverter.dbJsonToBean(dbConfig, false);

        for (DBGroup dbGroup : dbGroupList) {
            Optional<DBGroup> dbGroupOp = dbGroups.getDbGroup().stream().filter(sourceDbGroup -> StringUtil.equals(sourceDbGroup.getName(), dbGroup.getName())).findFirst();
            if (!dbGroupOp.isPresent()) {
                String msg = String.format("this row[%s] does not exist.", dbGroup.getName());
                throw new SQLException(msg, "42S22", ErrorCode.ER_NO_REFERENCED_ROW_2);
            }
            dbGroup.setDbInstance(dbGroupOp.get().getDbInstance());
            dbGroups.getDbGroup().removeIf(sourceDbGroup -> StringUtil.equals(sourceDbGroup.getName(), dbGroup.getName()));
            dbGroups.getDbGroup().add(dbGroup);
        }

        dbConfig = DBConverter.dbBeanToJson(dbGroups);
        DbleTempConfig.getInstance().setDbConfig(dbConfig);
        return affectPks.size();
    }


    @Override
    public int deleteRows(Set<LinkedHashMap<String, String>> affectPks) {
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

        DBConverter dbConverter = new DBConverter();
        RawJson dbConfig = DbleServer.getInstance().getConfig().getDbConfig();
        DbGroups dbGroups = dbConverter.dbJsonToBean(dbConfig, false);
        for (LinkedHashMap<String, String> affectPk : dbGroupRows) {
            dbGroups.getDbGroup().removeIf(dbGroup -> StringUtil.equals(dbGroup.getName(), affectPk.get(COLUMN_NAME)));
        }

        RawJson dbGroupJson = DBConverter.dbBeanToJson(dbGroups);
        DbleTempConfig.getInstance().setDbConfig(dbGroupJson);
        return affectPks.size();
    }


    public DBGroup transformRowToDBGroup(LinkedHashMap<String, String> values) {
        DBGroup dbGroup = new DBGroup();
        HeartBeat heartbeat = new HeartBeat();
        dbGroup.setHeartbeat(heartbeat);
        for (Map.Entry<String, String> latestVal : values.entrySet()) {
            String key = latestVal.getKey();
            String value = latestVal.getValue();
            switch (key) {
                case COLUMN_NAME:
                    dbGroup.setName(value);
                    break;
                case COLUMN_HEARTBEAT_STMT:
                    heartbeat.setValue(value);
                    break;
                case COLUMN_HEARTBEAT_TIMEOUT:
                    heartbeat.setTimeout(Integer.parseInt(value));
                    break;
                case COLUMN_HEARTBEAT_RETRY:
                    heartbeat.setErrorRetryCount(Integer.parseInt(value));
                    break;
                case COLUMN_KEEP_ALIVE:
                    heartbeat.setKeepAlive(Integer.parseInt(value));
                    break;
                case COLUMN_RW_SPLIT_MODE:
                    dbGroup.setRwSplitMode(Integer.parseInt(value));
                    break;
                case COLUMN_DELAY_THRESHOLD:
                    dbGroup.setDelayThreshold(Integer.parseInt(value));
                    break;
                case COLUMN_DISABLE_HA:
                    dbGroup.setDisableHA(value);
                    break;
                case DELAY_PERIOD_MILLIS:
                    dbGroup.setDelayPeriodMillis(Integer.parseInt(value));
                    break;
                case DELAY_DATABASE:
                    dbGroup.setDelayDatabase(String.valueOf(value));
                    break;

                default:
                    break;
            }
        }
        return dbGroup;
    }

    private void checkDeleteRule(Set<LinkedHashMap<String, String>> affectPks) {
        for (LinkedHashMap<String, String> affectPk : affectPks) {
            //check user-group
            DbleRwSplitEntry dbleRwSplitEntry = (DbleRwSplitEntry) ManagerSchemaInfo.getInstance().getTables().get(DbleRwSplitEntry.TABLE_NAME);
            boolean existUser = dbleRwSplitEntry.getRows().stream().anyMatch(entry -> entry.get(DbleRwSplitEntry.COLUMN_DB_GROUP).equals(affectPk.get(COLUMN_NAME)));
            if (existUser) {
                throw new ConfigException("Cannot delete or update a parent row: a foreign key constraint fails `dble_rw_split_entry`(`db_group`) REFERENCES `dble_db_group`(`name`)");
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
            if (row.containsKey(COLUMN_DISABLE_HA) && !StringUtil.isEmpty(row.get(COLUMN_DISABLE_HA))) {
                String disableHaStr = row.get(COLUMN_DISABLE_HA);
                if (!StringUtil.equalsIgnoreCase(disableHaStr, Boolean.FALSE.toString()) &&
                        !StringUtil.equalsIgnoreCase(disableHaStr, Boolean.TRUE.toString())) {
                    throw new ConfigException("Column 'disable_ha' values only support 'false' or 'true'.");
                }
            }
            if (row.containsKey(COLUMN_RW_SPLIT_MODE) && !StringUtil.isEmpty(row.get(COLUMN_RW_SPLIT_MODE))) {
                String rwSplitModeStr = row.get(COLUMN_RW_SPLIT_MODE);
                if (!StringUtil.isBlank(rwSplitModeStr)) {
                    int rwSplitMode = IntegerUtil.parseInt(rwSplitModeStr);
                    if (rwSplitMode > 3 || rwSplitMode < 0) {
                        throw new ConfigException("rwSplitMode should be between 0 and 3!");
                    }
                }
            }
            checkInterValue(row);
        }
    }

    private void checkInterValue(LinkedHashMap<String, String> row) {
        String delayThresholdStr = row.get(COLUMN_DELAY_THRESHOLD);
        String heartbeatTimeoutStr = row.get(COLUMN_HEARTBEAT_TIMEOUT);
        String heartbeatRetryStr = row.get(COLUMN_HEARTBEAT_RETRY);
        if (row.containsKey(COLUMN_DELAY_THRESHOLD) && (StringUtil.isBlank(delayThresholdStr) || IntegerUtil.parseInt(delayThresholdStr) < -1)) {
            throw new ConfigException("Column '" + COLUMN_DELAY_THRESHOLD + "' should be an integer greater than or equal to -1!");
        }
        if (row.containsKey(COLUMN_HEARTBEAT_TIMEOUT) && (StringUtil.isBlank(heartbeatTimeoutStr) || IntegerUtil.parseInt(heartbeatTimeoutStr) < 0)) {
            throw new ConfigException("Column '" + COLUMN_HEARTBEAT_TIMEOUT + "' should be an integer greater than or equal to 0!");
        }
        if (row.containsKey(COLUMN_HEARTBEAT_RETRY) && (StringUtil.isBlank(heartbeatRetryStr) || IntegerUtil.parseInt(heartbeatRetryStr) < 0)) {
            throw new ConfigException("Column '" + COLUMN_HEARTBEAT_RETRY + "' should be an integer greater than or equal to 0!");
        }
        String heartbeatKeepAliveStr = row.get(COLUMN_KEEP_ALIVE);
        if (row.containsKey(COLUMN_KEEP_ALIVE) && (StringUtil.isBlank(heartbeatKeepAliveStr) || IntegerUtil.parseInt(heartbeatKeepAliveStr) < 0)) {
            throw new ConfigException("Column '" + COLUMN_KEEP_ALIVE + "' should be an integer greater than or equal to 0!");
        }
        String delayPeriodMillis = row.get(DELAY_PERIOD_MILLIS);
        if (row.containsKey(DELAY_PERIOD_MILLIS) && (StringUtil.isBlank(delayPeriodMillis) || IntegerUtil.parseInt(delayPeriodMillis) < -1)) {
            throw new ConfigException("Column '" + DELAY_PERIOD_MILLIS + "' should be an integer greater than or equal to -1!");
        }
    }

    private LinkedHashMap<String, String> initMap(DbGroupConfig dbGroupConfig) {
        LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
        map.put(COLUMN_NAME, dbGroupConfig.getName());
        map.put(COLUMN_HEARTBEAT_STMT, dbGroupConfig.getHeartbeatSQL());
        map.put(COLUMN_HEARTBEAT_TIMEOUT, String.valueOf(dbGroupConfig.getHeartbeatTimeout() / 1000));
        map.put(COLUMN_HEARTBEAT_RETRY, String.valueOf(dbGroupConfig.getErrorRetryCount()));
        map.put(COLUMN_KEEP_ALIVE, String.valueOf(dbGroupConfig.getKeepAlive()));
        map.put(COLUMN_RW_SPLIT_MODE, String.valueOf(dbGroupConfig.getRwSplitMode()));
        map.put(COLUMN_DELAY_THRESHOLD, String.valueOf(dbGroupConfig.getDelayThreshold()));
        map.put(DELAY_PERIOD_MILLIS, String.valueOf(dbGroupConfig.getDelayPeriodMillis()));
        map.put(DELAY_DATABASE, String.valueOf(dbGroupConfig.getDelayDatabase()));
        map.put(COLUMN_DISABLE_HA, String.valueOf(dbGroupConfig.isDisableHA()));
        return map;
    }

    @Override
    public void updateTempConfig() {
        DbleTempConfig.getInstance().setDbConfig(DbleServer.getInstance().getConfig().getDbConfig());
    }
}
