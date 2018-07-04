/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.alarm.ToResolveContainer;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractTableMetaHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableMetaHandler.class);
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLS = new String[]{
            "Table",
            "Create Table"};
    private static final String SQL_PREFIX = "show create table ";

    protected String tableName;
    private List<String> dataNodes;
    private AtomicInteger nodesNumber;
    protected String schema;
    private Set<String> selfNode;
    private ConcurrentMap<String, List<String>> dataNodeTableStructureSQLMap;
    public AbstractTableMetaHandler(String schema, TableConfig tbConfig, Set<String> selfNode) {
        this(schema, tbConfig.getName(), tbConfig.getDataNodes(), selfNode);
    }

    public AbstractTableMetaHandler(String schema, String tableName, List<String> dataNodes, Set<String> selfNode) {
        this.dataNodes = dataNodes;
        this.nodesNumber = new AtomicInteger(dataNodes.size());
        this.schema = schema;
        this.selfNode = selfNode;
        this.tableName = tableName;
        this.dataNodeTableStructureSQLMap = new ConcurrentHashMap<>();
    }

    public void execute() {
        for (String dataNode : dataNodes) {
            if (selfNode != null && selfNode.contains(dataNode)) {
                this.countdown();
                return;
            }
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLTableStructureListener(dataNode, System.currentTimeMillis()));
            SQLJob sqlJob = new SQLJob(SQL_PREFIX + tableName, dataNode, resultHandler, false);
            sqlJob.run();
        }
    }

    protected abstract void countdown();

    protected abstract void handlerTable(StructureMeta.TableMeta tableMeta);

    private class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private String dataNode;
        private long version;

        MySQLTableStructureListener(String dataNode, long version) {
            this.dataNode = dataNode;
            this.version = version;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            String tableId = "DataNode[" + dataNode + "]:Table[" + tableName + "]";
            if (!result.isSuccess()) {
                //not thread safe
                String warnMsg = "Can't get table " + tableName + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!";
                LOGGER.warn(warnMsg);
                AlertUtil.alertSelf(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, warnMsg, AlertUtil.genSingleLabel("TABLE", tableId));
                ToResolveContainer.TABLE_LACK.add(tableId);
                if (nodesNumber.decrementAndGet() == 0) {
                    StructureMeta.TableMeta tableMeta = genTableMeta();
                    handlerTable(tableMeta);
                    countdown();
                }
                return;
            } else if (ToResolveContainer.TABLE_LACK.contains(tableId) &&
                    AlertUtil.alertSelfResolve(AlarmCode.TABLE_LACK, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId))) {
                ToResolveContainer.TABLE_LACK.remove(tableId);
            }
            String currentSql = result.getResult().get(MYSQL_SHOW_CREATE_TABLE_COLS[1]);
            if (dataNodeTableStructureSQLMap.containsKey(currentSql)) {
                List<String> dataNodeList = dataNodeTableStructureSQLMap.get(currentSql);
                dataNodeList.add(dataNode);
            } else {
                List<String> dataNodeList = new LinkedList<>();
                dataNodeList.add(dataNode);
                dataNodeTableStructureSQLMap.put(currentSql, dataNodeList);
            }

            if (nodesNumber.decrementAndGet() == 0) {
                StructureMeta.TableMeta tableMeta = genTableMeta();
                handlerTable(tableMeta);
                countdown();
            }
        }

        private StructureMeta.TableMeta genTableMeta() {
            StructureMeta.TableMeta tableMeta = null;
            if (dataNodeTableStructureSQLMap.size() > 1) {
                // Through the SQL is different, the table Structure may still same.
                // for example: autoIncrement number
                Set<StructureMeta.TableMeta> tableMetas = new HashSet<>();
                for (String sql : dataNodeTableStructureSQLMap.keySet()) {
                    tableMeta = initTableMeta(tableName, sql, version);
                    tableMetas.add(tableMeta);
                }
                String tableId = schema + "." + tableName;
                if (tableMetas.size() > 1) {
                    consistentWarning();
                } else if (ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.contains(tableId) &&
                        AlertUtil.alertSelfResolve(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, AlertUtil.genSingleLabel("TABLE", tableId))) {
                    ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.remove(tableId);
                }
                tableMetas.clear();
            } else if (dataNodeTableStructureSQLMap.size() == 1) {
                tableMeta = initTableMeta(tableName, dataNodeTableStructureSQLMap.keySet().iterator().next(), version);
            }
            return tableMeta;
        }

        private void consistentWarning() {
            String errorMsg = "Table [" + tableName + "] structure are not consistent in different data node!";
            LOGGER.warn(errorMsg);
            AlertUtil.alertSelf(AlarmCode.TABLE_NOT_CONSISTENT_IN_DATAHOSTS, Alert.AlertLevel.WARN, errorMsg, AlertUtil.genSingleLabel("TABLE", schema + "." + tableName));
            ToResolveContainer.TABLE_NOT_CONSISTENT_IN_DATAHOSTS.add(schema + "." + tableName);
            LOGGER.info("Currently detected: ");
            for (Map.Entry<String, List<String>> entry : dataNodeTableStructureSQLMap.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String dn : entry.getValue()) {
                    stringBuilder.append("DataNode:[").append(dn).append("]");
                }
                stringBuilder.append(":").append(entry);
                LOGGER.info(stringBuilder.toString());
            }
        }

        private StructureMeta.TableMeta initTableMeta(String table, String sql, long timeStamp) {
            try {
                SQLStatementParser parser = new CreateTableParserImp(sql);
                SQLCreateTableStatement createStatement = parser.parseCreateTable();
                return MetaHelper.initTableMeta(table, createStatement, timeStamp);

            } catch (Exception e) {
                LOGGER.warn("sql[" + sql + "] parser error:", e);
                AlertUtil.alertSelf(AlarmCode.GET_TABLE_META_FAIL, Alert.AlertLevel.WARN, "sql[" + sql + "] parser error:" + e.getMessage(), null);
                return null;
            }
        }
    }
}
