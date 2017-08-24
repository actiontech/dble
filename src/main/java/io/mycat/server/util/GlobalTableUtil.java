package io.mycat.server.util;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastjson.JSON;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.MySQLConsistencyChecker;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.protocol.StructureMeta.TableMeta;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author digdeep@126.com
 * 全局表一致性检查 和 拦截
 */
public final class GlobalTableUtil {
    private GlobalTableUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTableUtil.class);
    private static Map<String, TableConfig> globalTableMap = new ConcurrentHashMap<>();
    /**
     * 全局表 保存修改时间戳 的字段名，用于全局表一致性检查
     */
    public static final String GLOBAL_TABLE_CHECK_COLUMN = "_mycat_op_time";
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
        getGlobalTable();    // 初始化 globalTableMap
    }

    public static SQLColumnDefinition createCheckColumn() {
        SQLColumnDefinition column = new SQLColumnDefinition();
        column.setDataType(new SQLCharacterDataType("bigint"));
        column.setName(new SQLIdentifierExpr(GLOBAL_TABLE_CHECK_COLUMN));
        column.setComment(new SQLCharExpr("全局表保存修改时间戳的字段名"));
        return column;
    }

    public static boolean isInnerColExist(SchemaInfo schemaInfo, TableMeta orgTbMeta) {
        for (int i = 0; i < orgTbMeta.getColumnsList().size(); i++) {
            String column = orgTbMeta.getColumnsList().get(i).getName();
            if (column.equalsIgnoreCase(GLOBAL_TABLE_CHECK_COLUMN))
                return true;
        }
        StringBuilder warnStr = new StringBuilder(schemaInfo.schema).append(".").append(schemaInfo.table).
                append(" inner column: ").append(GLOBAL_TABLE_CHECK_COLUMN).append(" is not exist.");
        LOGGER.warn(warnStr.toString());
        return false; // tableName 全局表没有内部列
    }

    private static void getGlobalTable() {
        MycatConfig config = MycatServer.getInstance().getConfig();
        for (Map.Entry<String, SchemaConfig> entry : config.getSchemas().entrySet()) {
            for (TableConfig table : entry.getValue().getTables().values()) {
                if (table.isGlobalTable()) {
                    String tableName = table.getName();
                    if (config.getSystem().isLowerCaseTableNames()) {
                        tableName = tableName.toLowerCase();
                    }
                    globalTableMap.put(entry.getKey() + "." + tableName, table);
                }
            }
        }
    }

    public static void consistencyCheck() {
        MycatConfig config = MycatServer.getInstance().getConfig();
        for (TableConfig table : globalTableMap.values()) {
            Map<String, ArrayList<PhysicalDBNode>> executedMap = new HashMap<>();
            // <table name="travelrecord" dataNode="dn1,dn2,dn3"
            for (String nodeName : table.getDataNodes()) {
                Map<String, PhysicalDBNode> map = config.getDataNodes();
                for (PhysicalDBNode dBnode : map.values()) {
                    // <dataNode name="dn1" dataHost="localhost1" database="db1" />
                    if (nodeName.equals(dBnode.getName())) {    // dn1,dn2,dn3
                        PhysicalDBPool pool = dBnode.getDbPool();
                        Collection<PhysicalDatasource> allDS = pool.getAllDataSources();
                        for (PhysicalDatasource pds : allDS) {
                            if (pds instanceof MySQLDataSource) {
                                ArrayList<PhysicalDBNode> nodes = executedMap.get(pds.getName());
                                if (nodes == null) {
                                    nodes = new ArrayList<>();
                                }
                                nodes.add(dBnode);
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
                                LOGGER.warn(e.getMessage());
                            }
                        }
                        LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
                        // 一种 check 完成之后，再进行另一种 check
                        checker = new MySQLConsistencyChecker(mds, schemas, table.getName());
                        isColumnCountCheckFinished = 0;
                        checker.checkRecordCout();
                        while (isColumnCountCheckFinished <= 0) {
                            LOGGER.debug("isColumnCountCheckFinished:" + isColumnCountCheckFinished);
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                LOGGER.warn(e.getMessage());
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
     * 每次处理 一种 check 的结果，不会交叉同时处理 多种不同 check 的结果
     *
     * @param list
     * @return
     */
    public static List<SQLQueryResult<Map<String, String>>> finished(List<SQLQueryResult<Map<String, String>>> list) {
        LOCK.lock();
        try {
            //[{"dataNode":"db3","result":{"inner_col_exist":"id,_mycat_op_time"},"success":true,"tableName":"COMPANY"}]
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
                                columnsList = row.get(GlobalTableUtil.INNER_COLUMN); // id,name,_mycat_op_time
                            LOGGER.debug("columnsList: " + columnsList);
                        } catch (Exception e) {
                            LOGGER.warn(row.get(GlobalTableUtil.INNER_COLUMN) + ", " + e.getMessage());
                        } finally {
                            if (columnsList == null ||
                                    !columnsList.contains(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN)) {
                                LOGGER.warn(map.getDataNode() + "." + map.getTableName() +
                                        " inner column: " + GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN + " is not exist.");
                            } else {
                                LOGGER.debug("columnsList: " + columnsList);
                            }
//                            isInnerColumnCheckFinished = 1;
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

    public static boolean useGlobleTableCheck() {
        SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
        return system != null && system.getUseGlobleTableCheck() == 1;
    }

    public static boolean isGlobalTable(SchemaConfig schemaConfig, String tableName) {
        TableConfig table = schemaConfig.getTables().get(tableName);
        return table != null && table.isGlobalTable();
    }
}
