/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.ErrorCode;
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
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public final class DeleteHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHandler.class);

    public void handle(String stmt, ManagerService service) {
        MySqlDeleteStatement delete;
        try {
            delete = (MySqlDeleteStatement) (RouteStrategyFactory.getRouteStrategy().parserSQL(stmt));
        } catch (Exception e) {
            LOGGER.warn("manager parser delete failed", e);
            service.writeErrMessage("42000", "You have an error in your SQL syntax", ErrorCode.ER_PARSE_ERROR);
            return;
        }
        if (delete.isLowPriority() || delete.isQuick() || delete.isIgnore() || delete.getLimit() != null || delete.getOrderBy() != null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete with syntax :[LOW_PRIORITY] [QUICK] [IGNORE] ... [ORDER BY ...] [LIMIT row_count]");
            return;
        }
        if (delete.getWhere() == null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete without WHERE");
            return;
        }
        SQLTableSource tableSource = delete.getTableSource();
        if (tableSource instanceof SQLJoinTableSource) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete Multiple-Table");
            return;
        }
        SQLExprTableSource singleTableSource = (SQLExprTableSource) tableSource;
        if (singleTableSource.getAlias() != null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete with alias");
            return;
        }
        if (singleTableSource.getPartitionSize() != 0) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete with [PARTITION (partition_name [, partition_name] ...)]");
            return;
        }
        ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
        delete.accept(visitor);
        if (visitor.getNotSupportMsg() != null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, visitor.getNotSupportMsg());
            return;
        } else if (visitor.getFirstClassSubQueryList().size() > 0) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support sub-query");
            return;
        }
        SchemaUtil.SchemaInfo schemaInfo;
        try {
            schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), service.getSchema(), singleTableSource);
        } catch (SQLException e) {
            service.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        }
        ManagerBaseTable managerBaseTable = ManagerSchemaInfo.getInstance().getTables().get(schemaInfo.getTable());
        if (!managerBaseTable.isWritable()) {
            service.writeErrMessage("42000", "Access denied for table '" + managerBaseTable.getTableName() + "'", ErrorCode.ER_ACCESS_DENIED_ERROR);
            return;
        }
        ManagerWritableTable managerTable = (ManagerWritableTable) managerBaseTable;
        int rowSize;
        boolean lockFlag = managerTable.getLock().tryLock();
        if (!lockFlag) {
            service.writeErrMessage(ErrorCode.ER_YES, "Other threads are executing management commands(insert/update/delete), please try again later.");
            return;
        }
        try {
            List<RowDataPacket> foundRows = ManagerTableUtil.getFoundRows(service, managerTable, delete.getWhere());
            Set<LinkedHashMap<String, String>> affectPks = ManagerTableUtil.getAffectPks(service, managerTable, foundRows, null);
            rowSize = managerTable.deleteRows(affectPks);
            if (rowSize != 0) {
                ReloadConfig.execute(service, 0, false, new ConfStatus(ConfStatus.Status.MANAGER_DELETE, managerTable.getTableName()));
            }
        } catch (SQLException e) {
            service.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        } catch (ConfigException e) {
            service.writeErrMessage(ErrorCode.ER_YES, "Delete failure.The reason is " + e.getMessage());
            return;
        } catch (Exception e) {
            if (e.getCause() instanceof ConfigException) {
                //reload fail
                handleConfigException(e, service, managerTable);
            } else {
                service.writeErrMessage(ErrorCode.ER_YES, "unknown error:" + e.getMessage());
                LOGGER.warn("unknown error:", e);
            }
            return;
        } finally {
            managerTable.deleteBackupFile();
            managerTable.getLock().unlock();
        }
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(rowSize);
        ok.write(service.getConnection());
    }

    private void handleConfigException(Exception e, ManagerService service, ManagerWritableTable managerTable) {
        try {
            managerTable.rollbackXmlFile();
        } catch (IOException ioException) {
            service.writeErrMessage(ErrorCode.ER_YES, "unknown error:" + e.getMessage());
            return;
        }
        service.writeErrMessage(ErrorCode.ER_YES, "Delete failure.The reason is " + e.getMessage());
    }
}
