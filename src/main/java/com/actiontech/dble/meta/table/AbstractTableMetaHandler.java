/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.FormatUtil;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
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

    private String tableName;
    private List<String> dataNodes;
    private AtomicInteger nodesNumber;
    protected String schema;
    private Set<String> selfNode;

    public AbstractTableMetaHandler(String schema, TableConfig tbConfig, Set<String> selfNode) {
        this(schema, tbConfig.getName(), tbConfig.getDataNodes(), selfNode);
    }

    public AbstractTableMetaHandler(String schema, String tableName, List<String> dataNodes, Set<String> selfNode) {
        this.dataNodes = dataNodes;
        this.nodesNumber = new AtomicInteger(dataNodes.size());
        this.schema = schema;
        this.selfNode = selfNode;
        this.tableName = tableName;
    }

    public void execute() {
        for (String dataNode : dataNodes) {
            if (selfNode != null && selfNode.contains(dataNode)) {
                this.countdown();
                return;
            }
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLS, new MySQLTableStructureListener(dataNode, System.currentTimeMillis(), new ConcurrentHashMap<String, List<String>>()));
            PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
            SQLJob sqlJob = new SQLJob(SQL_PREFIX + tableName, dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
            sqlJob.run();
        }
    }

    protected abstract void countdown();

    protected abstract void handlerTable(StructureMeta.TableMeta tableMeta);

    private class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private String dataNode;
        private long version;
        private ConcurrentMap<String, List<String>> dataNodeTableStructureSQLMap;

        MySQLTableStructureListener(String dataNode, long version, ConcurrentMap<String, List<String>> dataNodeTableStructureSQLMap) {
            this.dataNode = dataNode;
            this.version = version;
            this.dataNodeTableStructureSQLMap = dataNodeTableStructureSQLMap;
        }

        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            if (!result.isSuccess()) {
                //not thread safe
                LOGGER.info("Can't get table " + tableName + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!");
                if (nodesNumber.decrementAndGet() == 0) {
                    countdown();
                }
                return;
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
                StructureMeta.TableMeta tableMeta = null;
                if (dataNodeTableStructureSQLMap.size() > 1) {
                    // Through the SQL is different, the table Structure may still same.
                    // for example: autoIncrement number
                    Set<StructureMeta.TableMeta> tableMetas = new HashSet<>();
                    for (String sql : dataNodeTableStructureSQLMap.keySet()) {
                        tableMeta = initTableMeta(tableName, sql, version);
                        tableMetas.add(tableMeta);
                    }
                    if (tableMetas.size() > 1) {
                        consistentWarning();
                    }
                    tableMetas.clear();
                } else {
                    tableMeta = initTableMeta(tableName, currentSql, version);
                }
                handlerTable(tableMeta);
                countdown();
            }
        }

        private void consistentWarning() {
            LOGGER.warn(AlarmCode.CORE_TABLE_CHECK_WARN + "Table [" + tableName + "] structure are not consistent!");
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
            sql = FormatUtil.deleteComment(sql);
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLCreateTableStatement createStatement = parser.parseCreateTable();
            return MetaHelper.initTableMeta(table, createStatement, timeStamp);
        }


    }
}
