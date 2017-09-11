/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
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
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLMS = new String[]{
            "Table",
            "Create Table"};
    private static final String SQL_PREFIX = "show create table ";


    private TableConfig tbConfig;
    private AtomicInteger nodesNumber;
    protected String schema;
    private Set<String> selfNode;

    public AbstractTableMetaHandler(String schema, TableConfig tbConfig, Set<String> selfNode) {
        this.tbConfig = tbConfig;
        this.nodesNumber = new AtomicInteger(tbConfig.getDataNodes().size());
        this.schema = schema;
        this.selfNode = selfNode;
    }

    public void execute() {
        for (String dataNode : tbConfig.getDataNodes()) {
            if (selfNode != null && selfNode.contains(dataNode)) {
                this.countdown();
                return;
            }
            try {
                tbConfig.getReentrantReadWriteLock().writeLock().lock();
                ConcurrentMap<String, List<String>> map = new ConcurrentHashMap<>();
                tbConfig.setDataNodeTableStructureSQLMap(map);
            } finally {
                tbConfig.getReentrantReadWriteLock().writeLock().unlock();
            }
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLMS, new MySQLTableStructureListener(dataNode, System.currentTimeMillis()));
            PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
            SQLJob sqlJob = new SQLJob(SQL_PREFIX + tbConfig.getName(), dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
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
            try {
                tbConfig.getReentrantReadWriteLock().writeLock().lock();
                if (!result.isSuccess()) {
                    //not thread safe
                    LOGGER.warn("Can't get table " + tbConfig.getName() + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!");
                    if (nodesNumber.decrementAndGet() == 0) {
                        countdown();
                    }
                    return;
                }
                String currentSql = result.getResult().get(MYSQL_SHOW_CREATE_TABLE_COLMS[1]);
                Map<String, List<String>> dataNodeTableStructureSQLMap = tbConfig.getDataNodeTableStructureSQLMap();
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
                        // for example: autoIncreament number
                        Set<StructureMeta.TableMeta> tableMetas = new HashSet<>();
                        for (String sql : dataNodeTableStructureSQLMap.keySet()) {
                            tableMeta = initTableMeta(tbConfig.getName(), sql, version);
                            tableMetas.add(tableMeta);
                        }
                        if (tableMetas.size() > 1) {
                            consistentWarning(dataNodeTableStructureSQLMap);
                        }
                        tableMetas.clear();
                    } else {
                        tableMeta = initTableMeta(tbConfig.getName(), currentSql, version);
                    }
                    handlerTable(tableMeta);
                    countdown();
                }
            } finally {
                tbConfig.getReentrantReadWriteLock().writeLock().unlock();
            }
        }

        private void consistentWarning(Map<String, List<String>> dataNodeTableStructureSQLMap) {
            LOGGER.warn("Table [" + tbConfig.getName() + "] structure are not consistent!");
            LOGGER.warn("Currently detected: ");
            for (Map.Entry<String, List<String>> entry : dataNodeTableStructureSQLMap.entrySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String dn : entry.getValue()) {
                    stringBuilder.append("DataNode:[").append(dn).append("]");
                }
                stringBuilder.append(":").append(entry);
                LOGGER.warn(stringBuilder.toString());
            }
        }

        private StructureMeta.TableMeta initTableMeta(String table, String sql, long timeStamp) {
            SQLStatementParser parser = new MySqlStatementParser(sql);
            SQLCreateTableStatement createStment = parser.parseCreateTable();
            return MetaHelper.initTableMeta(table, createStment, timeStamp);
        }
    }
}
