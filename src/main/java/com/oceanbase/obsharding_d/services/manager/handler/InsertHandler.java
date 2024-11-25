/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ConfStatus;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.meta.ReloadException;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.services.manager.information.ManagerWritableTable;
import com.oceanbase.obsharding_d.services.manager.response.ReloadConfig;
import com.oceanbase.obsharding_d.util.StringUtil;
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
        PacketResult packetResult = new PacketResult();
        if (ClusterConfig.getInstance().isClusterEnable()) {
            insertWithCluster(service, insert, managerTable, columns, packetResult);
        } else {
            generalInsert(service, insert, managerTable, columns, packetResult);
        }
        writePacket(packetResult.isSuccess(), packetResult.getRowSize(), service, packetResult.getErrorMsg());
    }

    private void insertWithCluster(ManagerService service, MySqlInsertStatement insert, ManagerWritableTable managerTable, List<String> columns, PacketResult packetResult) {
        //cluster-lock
        DistributeLock distributeLock;
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getConfChangeLockPath());
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is reloading, please try again later.");
            return;
        }
        LOGGER.info("insert OBsharding-D_information[{}]: added distributeLock {}", managerTable.getTableName(), ClusterMetaUtil.getConfChangeLockPath());
        try {
            generalInsert(service, insert, managerTable, columns, packetResult);
        } finally {
            distributeLock.release();
        }
    }

    private void generalInsert(ManagerService service, MySqlInsertStatement insert, ManagerWritableTable managerTable, List<String> columns, PacketResult packetResult) {
        //stand-alone lock
        List<LinkedHashMap<String, String>> rows;
        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        boolean lockFlag = lock.writeLock().tryLock();
        if (!lockFlag) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other threads are executing management commands(insert/update/delete), please try again later.");
            return;
        }
        int rowSize;
        try {
            rows = managerTable.makeInsertRows(columns, insert.getValuesList());
            managerTable.checkPrimaryKeyDuplicate(rows);
            rowSize = managerTable.insertRows(rows);
            if (rowSize != 0) {
                ReloadConfig.execute(service, 0, false, new ConfStatus(ConfStatus.Status.MANAGER_INSERT, managerTable.getTableName()));
            }
            managerTable.afterExecute();
        } catch (SQLException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
        } catch (ReloadException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
        } catch (ConfigException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Insert failure.The reason is " + e.getMessage());
        } catch (Exception e) {
            packetResult.setSuccess(false);
            if (e.getCause() instanceof ReloadException) {
                packetResult.setErrorMsg("Insert failure.The reason is " + e.getMessage());
                packetResult.setErrorCode(((ReloadException) e).getErrorCode());
            } else if (e.getCause() instanceof ConfigException) {
                packetResult.setErrorMsg("Insert failure.The reason is " + e.getMessage());
            } else {
                packetResult.setErrorMsg("unknown error:" + e.getMessage());
                LOGGER.warn("unknown error: {}", e.getMessage());
            }
        } finally {
            managerTable.updateTempConfig();
            lock.writeLock().unlock();
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

    private void writePacket(boolean isSuccess, int rowSize, ManagerService service, String errorMsg) {
        if (isSuccess) {
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(rowSize);
            ok.write(service.getConnection());
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, errorMsg);
        }
    }
}
