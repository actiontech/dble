/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ConfigException;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.actiontech.dble.services.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.services.manager.information.ManagerTableUtil;
import com.actiontech.dble.services.manager.information.ManagerWritableTable;
import com.actiontech.dble.services.manager.response.ReloadConfig;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public final class UpdateHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateHandler.class);

    public void handle(String stmt, ManagerService service) {
        MySqlUpdateStatement update;
        try {
            update = (MySqlUpdateStatement) (RouteStrategyFactory.getRouteStrategy().parserSQL(stmt));
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
        writePacket(packetResult.isSuccess(), packetResult.getRowSize(), service, packetResult.getErrorMsg(), packetResult.getSqlState(), packetResult.getErrorCode());
    }

    private void updateWithCluster(ManagerService service, MySqlUpdateStatement update, ManagerWritableTable managerTable, LinkedHashMap<String, String> values, PacketResult packetResult) {
        //cluster-lock
        DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getConfChangeLockPath(), SystemConfig.getInstance().getInstanceName());
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is reloading, please try again later.");
            return;
        }
        LOGGER.info("update dble_information[{}]: added distributeLock {}", managerTable.getTableName(), ClusterPathUtil.getConfChangeLockPath());
        try {
            generalUpdate(service, update, managerTable, values, packetResult);
        } finally {
            distributeLock.release();
        }
    }

    private void generalUpdate(ManagerService service, MySqlUpdateStatement update, ManagerWritableTable managerTable, LinkedHashMap<String, String> values, PacketResult packetResult) {
        boolean lockFlag = managerTable.getLock().tryLock();
        if (!lockFlag) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other threads are executing management commands(insert/update/delete), please try again later.");
            return;
        }
        try {
            int rowSize;
            try {
                List<RowDataPacket> foundRows = ManagerTableUtil.getFoundRows(service, managerTable, update.getWhere());
                Set<LinkedHashMap<String, String>> affectPks = ManagerTableUtil.getAffectPks(service, managerTable, foundRows, values);
                rowSize = updateRows(service, managerTable, affectPks, values, packetResult);
                packetResult.setRowSize(rowSize);
            } catch (SQLException e) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg(e.getMessage());
                packetResult.setSqlState(StringUtil.isEmpty(e.getSQLState()) ? "HY000" : e.getSQLState());
                packetResult.setErrorCode(e.getErrorCode());
            } catch (ConfigException e) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Update failure.The reason is " + e.getMessage());
            } catch (Exception e) {
                packetResult.setSuccess(false);
                if (e.getCause() instanceof ConfigException) {
                    //reload fail
                    packetResult.setErrorMsg("Update failure.The reason is " + e.getMessage());
                    LOGGER.warn("Update failure.The reason is ", e);
                } else {
                    packetResult.setErrorMsg("unknown error:" + e.getMessage());
                    LOGGER.warn("unknown error:", e);
                }
            }
        } finally {
            DbleTempConfig.getInstance().setDbConfig(DbleServer.getInstance().getConfig().getDbConfig());
            DbleTempConfig.getInstance().setUserConfig(DbleServer.getInstance().getConfig().getUserConfig());
            managerTable.getLock().unlock();
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
        if (!managerBaseTable.isWritable()) {
            service.writeErrMessage("42000", "Access denied for table '" + managerBaseTable.getTableName() + "'", ErrorCode.ER_ACCESS_DENIED_ERROR);
            return null;
        }
        return (ManagerWritableTable) managerBaseTable;
    }

    private int updateRows(ManagerService service, ManagerWritableTable managerTable, Set<LinkedHashMap<String, String>> affectPks, LinkedHashMap<String, String> values, PacketResult packetResult) throws Exception {
        int rowSize = 0;
        if (!affectPks.isEmpty()) {
            rowSize = managerTable.updateRows(affectPks, values);
            if (rowSize != 0) {
                ReloadConfig.execute(service, 0, false, new ConfStatus(ConfStatus.Status.MANAGER_UPDATE, managerTable.getTableName()), packetResult);
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

    private void writePacket(boolean isSuccess, int rowSize, ManagerService service, String errorMsg, String sqlState, int errorCode) {
        if (isSuccess) {
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(rowSize);
            ok.write(service.getConnection());
        } else if (!Strings.isNullOrEmpty(sqlState)) {
            service.writeErrMessage(sqlState, errorMsg, errorCode);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, errorMsg);
        }
    }
}
