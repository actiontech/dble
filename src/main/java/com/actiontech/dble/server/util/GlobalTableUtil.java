/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.heartbeat.MySQLConsistencyChecker;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author digdeep@126.com
 *         check GlobalTable consistency
 */
public final class GlobalTableUtil {
    private GlobalTableUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTableUtil.class);
    private static Map<String, TableConfig> globalTableMap = new ConcurrentHashMap<>();
    /**
     * the field of timestamp,
     * for check GlobalTable consistency
     */
    public static final String GLOBAL_TABLE_CHECK_COLUMN = "_dble_op_time";
    public static final String COUNT_COLUMN = "record_count";
    public static final String MAX_COLUMN = "max_timestamp";
    public static final String INNER_COLUMN = "inner_col_exist";
    private static volatile int isInnerColumnCheckFinished = 0;
    private static volatile int isColumnCountCheckFinished = 0;
    private static final ReentrantLock LOCK = new ReentrantLock(false);

    public static Map<String, TableConfig> getGlobalTableMap() {
        return globalTableMap;
    }

    static {
        getGlobalTable();    // init globalTableMap
    }

    public static SQLColumnDefinition createCheckColumn() {
        SQLColumnDefinition column = new SQLColumnDefinition();
        column.setDataType(new SQLCharacterDataType("bigint"));
        column.setDefaultExpr(new SQLNullExpr());
        column.setName(new SQLIdentifierExpr(GLOBAL_TABLE_CHECK_COLUMN));
        column.setComment(new SQLCharExpr("field for checking consistency"));
        return column;
    }

    public static boolean isInnerColExist(SchemaUtil.SchemaInfo schemaInfo, StructureMeta.TableMeta orgTbMeta) {
        String tableId = schemaInfo.getSchema() + "." + schemaInfo.getTable();
        for (int i = 0; i < orgTbMeta.getColumnsList().size(); i++) {
            String column = orgTbMeta.getColumnsList().get(i).getName();
            if (column.equalsIgnoreCase(GLOBAL_TABLE_CHECK_COLUMN)) {
                if (ToResolveContainer.GLOBAL_TABLE_COLUMN_LOST.contains(tableId)) {
                    AlertUtil.alertSelfResolve(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                            ToResolveContainer.GLOBAL_TABLE_COLUMN_LOST, tableId);
                }
                return true;
            }
        }
        String warnStr = tableId + " inner column: " + GLOBAL_TABLE_CHECK_COLUMN + " is not exist.";
        LOGGER.warn(warnStr);
        AlertUtil.alertSelf(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, warnStr, AlertUtil.genSingleLabel("TABLE", tableId));
        ToResolveContainer.GLOBAL_TABLE_COLUMN_LOST.add(tableId);
        return false; // tableName without inner column
    }

    private static void getGlobalTable() {
        ServerConfig config = DbleServer.getInstance().getConfig();
        for (Map.Entry<String, SchemaConfig> entry : config.getSchemas().entrySet()) {
            for (TableConfig table : entry.getValue().getTables().values()) {
                if (table.isGlobalTable()) {
                    String tableName = table.getName();
                    if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                        tableName = tableName.toLowerCase();
                    }
                    globalTableMap.put(entry.getKey() + "." + tableName, table);
                }
            }
        }
    }

    public static void consistencyCheck() {
        ServerConfig config = DbleServer.getInstance().getConfig();
        for (TableConfig table : globalTableMap.values()) {
            Map<String, ArrayList<PhysicalDBNode>> executedMap = new HashMap<>();
            // <table name="travelrecord" dataNode="dn1,dn2,dn3"
            for (String nodeName : table.getDataNodes()) {
                Map<String, PhysicalDBNode> map = config.getDataNodes();
                for (PhysicalDBNode dbNode : map.values()) {
                    // <dataNode name="dn1" dataHost="localhost1" database="db1" />
                    if (nodeName.equals(dbNode.getName())) {    // dn1,dn2,dn3
                        PhysicalDBPool pool = dbNode.getDbPool();
                        Collection<PhysicalDatasource> allDS = pool.getAllDataSources();
                        for (PhysicalDatasource pds : allDS) {
                            if (pds instanceof MySQLDataSource) {
                                ArrayList<PhysicalDBNode> nodes = executedMap.get(pds.getName());
                                if (nodes == null) {
                                    nodes = new ArrayList<>();
                                }
                                nodes.add(dbNode);
                                executedMap.put(pds.getName(), nodes);
                            }
                        }
                    }
                }
            }
            for (Map.Entry<String, ArrayList<PhysicalDBNode>> entry : executedMap.entrySet()) {
                ArrayList<PhysicalDBNode> nodes = entry.getValue();
                String[] schemas = new String[nodes.size()];
                for (int index = 0; index < nodes.size(); index++) {
                    schemas[index] = StringUtil.removeBackQuote(nodes.get(index).getDatabase());
                }
                Collection<PhysicalDatasource> allDS = nodes.get(0).getDbPool().getAllDataSources();
                for (PhysicalDatasource pds : allDS) {
                    if (pds instanceof MySQLDataSource && entry.getKey().equals(pds.getName())) {
                        MySQLDataSource mds = (MySQLDataSource) pds;
                        MySQLConsistencyChecker checker = new MySQLConsistencyChecker(mds, schemas, table.getName());
                        isInnerColumnCheckFinished = 0;
                        checker.checkInnerColumnExist();
                        while (isInnerColumnCheckFinished <= 0) {
                            LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                LOGGER.info(e.getMessage());
                            }
                        }
                        LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
                        // check another measure
                        checker = new MySQLConsistencyChecker(mds, schemas, table.getName());
                        isColumnCountCheckFinished = 0;
                        checker.checkRecordCount();
                        while (isColumnCountCheckFinished <= 0) {
                            LOGGER.debug("isColumnCountCheckFinished:" + isColumnCountCheckFinished);
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                LOGGER.info(e.getMessage());
                            }
                        }
                        LOGGER.debug("isColumnCountCheckFinished:" + isColumnCountCheckFinished);
                        checker = new MySQLConsistencyChecker(mds, schemas, table.getName());
                        checker.checkMaxTimeStamp();
                    }
                }
            }
        }
    }

    /**
     * check one measure for one time
     *
     * @param list
     * @return
     */
    public static List<SQLQueryResult<Map<String, String>>> finished(List<SQLQueryResult<Map<String, String>>> list) {
        LOCK.lock();
        try {
            //[{"dataNode":"db3","result":{"inner_col_exist":"id,_dble_op_time"},"success":true,"tableName":"COMPANY"}]
            // {"dataNode":"db2","result":{"max_timestamp":"1450423751170"},"success":true,"tableName":"COMPANY"}
            // {"dataNode":"db3","result":{"record_count":"1"},"success":true,"tableName":"COMPANY"}
            LOGGER.debug("list:::::::::::" + JSON.toJSONString(list));
            for (SQLQueryResult<Map<String, String>> map : list) {
                Map<String, String> row = map.getResult();
                if (row != null) {
                    if (row.containsKey(GlobalTableUtil.MAX_COLUMN)) {
                        LOGGER.info(map.getDataNode() + "." + map.getTableName() +
                                "." + GlobalTableUtil.MAX_COLUMN +
                                ": " + map.getResult().get(GlobalTableUtil.MAX_COLUMN));
                    }
                    if (row.containsKey(GlobalTableUtil.COUNT_COLUMN)) {
                        LOGGER.info(map.getDataNode() + "." + map.getTableName() +
                                "." + GlobalTableUtil.COUNT_COLUMN +
                                ": " + map.getResult().get(GlobalTableUtil.COUNT_COLUMN));
                    }
                    if (row.containsKey(GlobalTableUtil.INNER_COLUMN)) {
                        String columnsList = null;
                        try {
                            if (StringUtils.isNotBlank(row.get(GlobalTableUtil.INNER_COLUMN)))
                                columnsList = row.get(GlobalTableUtil.INNER_COLUMN); // id,name,_dble_op_time
                            LOGGER.debug("columnsList: " + columnsList);
                        } catch (Exception e) {
                            LOGGER.info(row.get(GlobalTableUtil.INNER_COLUMN) + ", " + e.getMessage());
                        } finally {
                            String tableId = map.getDataNode() + "." + map.getTableName();
                            if (columnsList == null || !columnsList.contains(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN)) {
                                String warnMsg = tableId + " inner column: " + GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN + " is not exist.";
                                LOGGER.warn(warnMsg);
                                AlertUtil.alertSelf(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                                ToResolveContainer.GLOBAL_TABLE_COLUMN_LOST.add(tableId);
                            } else {
                                if (ToResolveContainer.GLOBAL_TABLE_COLUMN_LOST.contains(tableId)) {
                                    AlertUtil.alertSelfResolve(AlarmCode.GLOBAL_TABLE_COLUMN_LOST, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId),
                                            ToResolveContainer.GLOBAL_TABLE_COLUMN_LOST, tableId);
                                }
                                LOGGER.debug("columnsList: " + columnsList);
                            }
                        }
                    }
                }
            }
        } finally {
            isInnerColumnCheckFinished = 1;
            isColumnCountCheckFinished = 1;
            LOCK.unlock();
        }
        return list;
    }

    public static boolean useGlobalTableCheck() {
        SystemConfig system = DbleServer.getInstance().getConfig().getSystem();
        return system != null && system.getUseGlobleTableCheck() == 1;
    }

    public static boolean isGlobalTable(SchemaConfig schemaConfig, String tableName) {
        TableConfig table = schemaConfig.getTables().get(tableName);
        return table != null && table.isGlobalTable();
    }
}
