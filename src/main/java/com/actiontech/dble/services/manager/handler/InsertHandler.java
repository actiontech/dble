/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.parser.util.DruidUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.services.manager.response.ReloadConfig;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class InsertHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertHandler.class);

    public void handle(String stmt, ManagerService service) {
        MySqlInsertStatement insert;
        try {
            insert = (MySqlInsertStatement) DruidUtil.parseMultiSQL(stmt);
        } catch (Exception e) {
            LOGGER.warn("manager parser insert failed", e);
            service.writeErrMessage("42000", "You have an error in your SQL syntax", ErrorCode.ER_PARSE_ERROR);
            return;
        }

        ManagerWritableTable managerTable = getWritableTable(insert, service);
        if (null == managerTable) {
            return;
        }
        List<String> columns = getColumn(insert, managerTable, service);
        if (null == columns) {
            return;
        }
        //cluster-lock
        DistributeLock distributeLock = null;
        if (ClusterConfig.getInstance().isClusterEnable()) {
            ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
            distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getConfChangeLockPath());
            if (!distributeLock.acquire()) {
                service.writeErrMessage(ErrorCode.ER_YES, "Other instance are executing reload config or management commands(insert/update/delete), please try again later.");
                return;
            }
            LOGGER.info("insert dble_information[{}]: added distributeLock {}", managerTable.getTableName(), ClusterMetaUtil.getConfChangeLockPath());
        }
        //stand-alone lock
        List<LinkedHashMap<String, String>> rows;
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        boolean lockFlag = lock.writeLock().tryLock();
        if (!lockFlag) {
            service.writeErrMessage(ErrorCode.ER_YES, "Other threads are executing reload config or management commands(insert/update/delete), please try again later.");
            return;
        }
        int rowSize = 0;
        boolean isSuccess = true;
        String errorMsg = null;
        try {
            rows = managerTable.makeInsertRows(columns, insert.getValuesList());
            managerTable.checkPrimaryKeyDuplicate(rows);
            rowSize = managerTable.insertRows(rows);
            if (rowSize != 0) {
                ReloadConfig.execute(service, 0, false, new ConfStatus(ConfStatus.Status.MANAGER_INSERT, managerTable.getTableName()));
            }
            managerTable.afterExecute();
        } catch (SQLException e) {
            isSuccess = false;
            errorMsg = e.getMessage();
        } catch (ConfigException e) {
            isSuccess = false;
            errorMsg = "Insert failure.The reason is " + e.getMessage();
        } catch (Exception e) {
            isSuccess = false;
            if (e.getCause() instanceof ConfigException) {
                errorMsg = "Insert failure.The reason is " + e.getMessage();
                LOGGER.warn("Insert failure.The reason is ", e);
            } else {
                errorMsg = "unknown error:" + e.getMessage();
                LOGGER.warn("unknown error:", e);
            }
        } finally {
            managerTable.updateTempConfig();
            lock.writeLock().unlock();
            if (distributeLock != null) {
                distributeLock.release();
            }
        }
        if (isSuccess) {
            writeOkPacket(1, rowSize, managerTable.getMsg(), service);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, errorMsg);
        }
    }

    private List<String> getColumn(MySqlInsertStatement insert, ManagerWritableTable managerTable, ManagerService service) {
        List<String> columns;
        if (insert.getColumns() == null || insert.getColumns().size() == 0) {
            columns = new ArrayList<>(managerTable.getColumnsMeta().size());
            for (ColumnMeta columnMeta : managerTable.getColumnsMeta()) {
                columns.add(columnMeta.getName());
            }
        } else {
            columns = new ArrayList<>(insert.getColumns().size());
            // columns contains all not null
            LinkedHashSet<String> mustSetColumns = new LinkedHashSet<>(managerTable.getMustSetColumns());
            for (int i = 0; i < insert.getColumns().size(); i++) {
                String columnName = StringUtil.removeBackQuote(insert.getColumns().get(i).toString()).toLowerCase();
                if (managerTable.getColumnType(columnName) == null) {
                    service.writeErrMessage("42S22", "Unknown column '" + columnName + "' in 'field list'", ErrorCode.ER_BAD_FIELD_ERROR);
                    return null;
                }
                mustSetColumns.remove(columnName);
                columns.add(columnName);
            }
            if (mustSetColumns.size() != 0) {
                service.writeErrMessage("HY000", "Field '" + mustSetColumns + "' doesn't have a default value and cannot be null", ErrorCode.ER_NO_DEFAULT_FOR_FIELD);
                return null;
            }
        }
        for (int i = 0; i < insert.getValuesList().size(); i++) {
            List<SQLExpr> value = insert.getValuesList().get(i).getValues();
            // checkout value size
            if (value.size() != columns.size()) {
                service.writeErrMessage("21S01", "Column count doesn't match value count at row " + (i + 1), ErrorCode.ER_WRONG_VALUE_COUNT_ON_ROW);
                return null;
            }
            for (int j = 0; j < value.size(); j++) {
                // value is null
                if (value.get(j) instanceof SQLNullExpr && managerTable.getNotNullColumns().contains(columns.get(j))) {
                    service.writeErrMessage("23000", "Column '" + columns.get(j) + "' cannot be null", ErrorCode.ER_BAD_NULL_ERROR);
                    return null;
                }
            }
        }
        return columns;
    }

    private void writeOkPacket(int i, int rowSize, String msg, ManagerService service) {
        OkPacket ok = new OkPacket();
        ok.setPacketId(i);
        ok.setAffectedRows(rowSize);
        if (!StringUtil.isEmpty(msg)) {
            ok.setMessage(StringUtil.encode(msg, service.getCharset().getResults()));
        }
        ok.write(service.getConnection());
    }

    private ManagerWritableTable getWritableTable(MySqlInsertStatement insert, ManagerService service) {
        if (insert.isLowPriority() || insert.isDelayed() || insert.isHighPriority() || insert.isIgnore() || (insert.getDuplicateKeyUpdate() != null && !insert.getDuplicateKeyUpdate().isEmpty())) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support insert with syntax :[LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE][ON DUPLICATE KEY UPDATE assignment_list]");
            return null;
        }
        if (insert.getQuery() != null) {
            service.writeErrMessage("42000", "Insert syntax error,not support insert ... select", ErrorCode.ER_PARSE_ERROR);
            return null;
        }
        if (insert.getValuesList().isEmpty()) {
            service.writeErrMessage("42000", "Insert syntax error,no values in sql", ErrorCode.ER_PARSE_ERROR);
            return null;
        }

        SQLExprTableSource tableSource = insert.getTableSource();
        if (tableSource.getPartitionSize() != 0) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support insert with syntax :[PARTITION (partition_name [, partition_name] ...)]");
            return null;
        }
        SchemaUtil.SchemaInfo schemaInfo;
        try {
            schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), service.getSchema(), tableSource);
        } catch (SQLException e) {
            service.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return null;
        }
        ManagerBaseTable managerBaseTable = ManagerSchemaInfo.getInstance().getTables().get(schemaInfo.getTable());
        if (managerBaseTable == null || !managerBaseTable.isWritable()) {
            service.writeErrMessage("42000", "Access denied for table '" + schemaInfo.getTable() + "'", ErrorCode.ER_ACCESS_DENIED_ERROR);
            return null;
        }
        return (ManagerWritableTable) managerBaseTable;
    }
}
