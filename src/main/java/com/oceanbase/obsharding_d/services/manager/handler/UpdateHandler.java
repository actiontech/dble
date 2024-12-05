/*
 * Copyright (C) 2016-2023 OBsharding_D.
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
import com.oceanbase.obsharding_d.meta.ReloadException;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.util.DruidUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.oceanbase.obsharding_d.services.manager.information.ManagerSchemaInfo;
import com.oceanbase.obsharding_d.services.manager.information.ManagerTableUtil;
import com.oceanbase.obsharding_d.services.manager.information.ManagerWritableTable;
import com.oceanbase.obsharding_d.services.manager.response.ReloadConfig;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class UpdateHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateHandler.class);

    public void handle(String stmt, ManagerService service) {
        MySqlUpdateStatement update;
        try {
            update = (MySqlUpdateStatement) DruidUtil.parseMultiSQL(stmt);
        } catch (Exception e) {
            LOGGER.warn("manager parser insert failed", e);
            service.writeErrMessage("42000", "You have an error in your SQL syntax", ErrorCode.ER_PARSE_ERROR);
            return;
        }
        ManagerWritableTable managerTable = getWritableTable(update, service);
        if (null == managerTable) {
            return;
        }
        LinkedHashMap<String, String> values;
        try {
            values = getUpdateValues(managerTable, update.getItems());
        } catch (SQLException e) {
            service.writeErrMessage(StringUtil.isEmpty(e.getSQLState()) ? "HY000" : e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        }
        PacketResult packetResult = new PacketResult();
        if (ClusterConfig.getInstance().isClusterEnable()) {
            updateWithCluster(service, update, managerTable, values, packetResult);
        } else {
            generalUpdate(service, update, managerTable, values, packetResult);
        }
        writePacket(packetResult.isSuccess(), packetResult.getRowSize(), service, packetResult.getErrorMsg());
    }

    private void updateWithCluster(ManagerService service, MySqlUpdateStatement update, ManagerWritableTable managerTable, LinkedHashMap<String, String> values, PacketResult packetResult) {
        //cluster-lock
        DistributeLock distributeLock;
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getConfChangeLockPath());
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is reloading, please try again later.");
            return;
        }
        LOGGER.info("update OBsharding-D_information[{}]: added distributeLock {}", managerTable.getTableName(), ClusterMetaUtil.getConfChangeLockPath());
        try {
            generalUpdate(service, update, managerTable, values, packetResult);
        } finally {
            distributeLock.release();
        }
    }

    private void generalUpdate(ManagerService service, MySqlUpdateStatement update, ManagerWritableTable managerTable, LinkedHashMap<String, String> values, PacketResult packetResult) {
        //stand-alone lock
        int rowSize;
        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        boolean lockFlag = lock.writeLock().tryLock();
        if (!lockFlag) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other threads are executing management commands(insert/update/delete), please try again later.");
            return;
        }
        try {
            List<RowDataPacket> foundRows = ManagerTableUtil.getFoundRows(service, managerTable, update.getWhere());
            Set<LinkedHashMap<String, String>> affectPks = ManagerTableUtil.getAffectPks(service, managerTable, foundRows, values);
            rowSize = updateRows(service, managerTable, affectPks, values);
            packetResult.setRowSize(rowSize);
        } catch (SQLException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
        } catch (ReloadException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
        } catch (ConfigException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Update failure.The reason is " + e.getMessage());
        } catch (Exception e) {
            packetResult.setSuccess(false);
            if (e.getCause() instanceof ReloadException) {
                packetResult.setErrorMsg("Update failure.The reason is " + e.getMessage());
                packetResult.setErrorCode(((ReloadException) e).getErrorCode());
            } else if (e.getCause() instanceof ConfigException) {
                packetResult.setErrorMsg("Update failure.The reason is " + e.getMessage());
            } else {
                packetResult.setErrorMsg("unknown error:" + e.getMessage());
                LOGGER.warn("unknown error: {}", e.getMessage());
            }
        } finally {
            managerTable.updateTempConfig();
            lock.writeLock().unlock();
        }
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


    public ManagerWritableTable getWritableTable(MySqlUpdateStatement update, ManagerService service) {
        if (update.getLimit() != null || update.isIgnore() || update.isLowPriority() || update.getOrderBy() != null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support update with syntax :[LOW_PRIORITY] [IGNORE] ... [ORDER BY ...] [LIMIT row_count]");
            return null;
        }
        if (update.getWhere() == null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support update without WHERE");
            return null;
        }
        SQLTableSource tableSource = update.getTableSource();
        if (tableSource instanceof SQLJoinTableSource) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support update Multiple-Table ");
            return null;
        }
        SQLExprTableSource singleTableSource = (SQLExprTableSource) tableSource;
        if (singleTableSource.getAlias() != null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support update with alias");
            return null;
        }
        if (singleTableSource.getPartitionSize() != 0) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support update with [PARTITION (partition_name [, partition_name] ...)]");
            return null;
        }
        ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
        update.accept(visitor);
        if (visitor.getNotSupportMsg() != null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, visitor.getNotSupportMsg());
            return null;
        } else if (visitor.getFirstClassSubQueryList().size() > 0) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "update syntax error, not support sub-query");
            return null;
        }
        SchemaUtil.SchemaInfo schemaInfo;
        try {
            schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), service.getSchema(), singleTableSource);
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

    private int updateRows(ManagerService service, ManagerWritableTable managerTable, Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values) throws Exception {
        int rowSize = 0;
        if (!affectPks.isEmpty()) {
            rowSize = managerTable.updateRows(affectPks, values);
            if (rowSize != 0) {
                ReloadConfig.execute(service, 0, false, new ConfStatus(ConfStatus.Status.MANAGER_UPDATE, managerTable.getTableName()));
            }
        }
        return rowSize;
    }

    private LinkedHashMap<String, String> getUpdateValues(ManagerWritableTable managerTable, List<SQLUpdateSetItem> updateItems) throws SQLException {
        LinkedHashMap<String, String> values = new LinkedHashMap<>(updateItems.size());
        for (SQLUpdateSetItem item : updateItems) {
            String columnName = getColumnName(item.getColumn().toString().toLowerCase(), managerTable.getTableName());
            if (managerTable.getColumnType(columnName) == null) {
                throw new SQLException("Unknown column '" + columnName + "' in 'field list'", "42S22", ErrorCode.ER_BAD_FIELD_ERROR);
            }
            if (managerTable.getPrimaryKeyColumns().contains(columnName)) {
                throw new SQLException("Primary column '" + columnName + "' can not be update, please use delete & insert", "42S22", ErrorCode.ER_BAD_FIELD_ERROR);
            }
            if (item.getValue() instanceof SQLNullExpr && managerTable.getNotNullColumns().contains(columnName)) {
                throw new SQLException("Column '" + columnName + "' cannot be null ", "23000", ErrorCode.ER_BAD_NULL_ERROR);
            }
            if (managerTable.getNotWritableColumnSet().contains(columnName)) {
                throw new SQLException("Column '" + columnName + "' is not writable", "42S22", ErrorCode.ER_ERROR_ON_WRITE);
            }
            if (managerTable.getLogicalPrimaryKeySet().contains(columnName)) {
                throw new SQLException("Column '" + columnName + "' is not writable.Because of the logical primary key " + new Gson().toJson(managerTable.getLogicalPrimaryKeySet()), "42S22", ErrorCode.ER_ERROR_ON_WRITE);
            }
            values.put(columnName, ManagerTableUtil.valueToString(item.getValue()));
        }
        return values;
    }

    private String getColumnName(String columnName, String expectTableName) throws SQLException {
        if (columnName.contains(StringUtil.TABLE_COLUMN_SEPARATOR)) {
            String[] columnInfo = columnName.split("\\.");
            if (columnInfo.length > 3) {
                throw new SQLException("Unknown column '" + columnName + "' in 'field list'", "42S22", ErrorCode.ER_BAD_FIELD_ERROR);
            } else if (columnInfo.length == 3) {
                if (!StringUtil.removeBackQuote(columnInfo[0]).equals(ManagerSchemaInfo.SCHEMA_NAME)) {
                    throw new SQLException("Unknown column '" + columnName + "' in 'field list'", "42S22", ErrorCode.ER_BAD_FIELD_ERROR);
                }
                if (!StringUtil.removeBackQuote(columnInfo[1]).equals(expectTableName)) {
                    throw new SQLException("Unknown column '" + columnName + "' in 'field list'", "42S22", ErrorCode.ER_BAD_FIELD_ERROR);
                }
                columnName = StringUtil.removeBackQuote(columnInfo[2]);
            } else {
                if (!StringUtil.removeBackQuote(columnInfo[0]).equals(expectTableName)) {
                    throw new SQLException("Unknown column '" + columnName + "' in 'field list'", "42S22", ErrorCode.ER_BAD_FIELD_ERROR);
                }
                columnName = StringUtil.removeBackQuote(columnInfo[1]);
            }
        }
        columnName = StringUtil.removeBackQuote(columnName);
        return columnName;
    }
}
