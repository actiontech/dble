/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.information.ManagerBaseTable;
import com.actiontech.dble.manager.information.ManagerSchemaInfo;
import com.actiontech.dble.manager.information.ManagerTableUtil;
import com.actiontech.dble.manager.information.ManagerWritableTable;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.factory.RouteStrategyFactory;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.server.util.SchemaUtil;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public final class DeleteHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHandler.class);
    public void handle(String stmt, ManagerConnection c) {
        MySqlDeleteStatement delete;
        try {
            delete = (MySqlDeleteStatement) (RouteStrategyFactory.getRouteStrategy().parserSQL(stmt));
        } catch (Exception e) {
            LOGGER.warn("manager parser delete failed", e);
            c.writeErrMessage("42000", "You have an error in your SQL syntax", ErrorCode.ER_PARSE_ERROR);
            return;
        }
        if (delete.isLowPriority() || delete.isQuick() || delete.isIgnore() || delete.getLimit() != null || delete.getOrderBy() != null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete with syntax :[LOW_PRIORITY] [QUICK] [IGNORE] ... [ORDER BY ...] [LIMIT row_count]");
            return;
        }
        if (delete.getWhere() == null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete without WHERE");
            return;
        }
        SQLTableSource tableSource = delete.getTableSource();
        if (tableSource instanceof SQLJoinTableSource) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete Multiple-Table");
            return;
        }
        SQLExprTableSource singleTableSource = (SQLExprTableSource) tableSource;
        if (singleTableSource.getAlias() != null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete with alias");
            return;
        }
        if (singleTableSource.getPartitionSize() != 0) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support delete with [PARTITION (partition_name [, partition_name] ...)]");
            return;
        }
        ServerSchemaStatVisitor visitor = new ServerSchemaStatVisitor();
        delete.accept(visitor);
        if (visitor.getNotSupportMsg() != null) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, visitor.getNotSupportMsg());
            return;
        } else if (visitor.getFirstClassSubQueryList().size() > 0) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "delete syntax error, not support sub-query");
            return;
        }
        SchemaUtil.SchemaInfo schemaInfo;
        try {
            schemaInfo = SchemaUtil.getSchemaInfo(c.getUser(), c.getSchema(), singleTableSource);
        } catch (SQLException e) {
            c.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        }
        ManagerBaseTable managerBaseTable = ManagerSchemaInfo.getInstance().getTables().get(schemaInfo.getTable());
        if (!managerBaseTable.isWritable()) {
            c.writeErrMessage("42000", "Access denied for table '" + managerBaseTable.getTableName() + "'", ErrorCode.ER_ACCESS_DENIED_ERROR);
            return;
        }
        ManagerWritableTable managerTable = (ManagerWritableTable) managerBaseTable;
        int rowSize;
        managerTable.getLock().lock();
        try {
            List<RowDataPacket> foundRows = ManagerTableUtil.getFoundRows(c, managerTable, delete.getWhere());
            Set<LinkedHashMap<String, String>> affectPks = ManagerTableUtil.getAffectPks(c, managerTable, foundRows);
            rowSize = managerTable.deleteRows(affectPks);
        } catch (SQLException e) {
            c.writeErrMessage(e.getSQLState(), e.getMessage(), e.getErrorCode());
            return;
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_YES, "unknown error:" + e.getMessage());
            return;
        } finally {
            managerTable.getLock().unlock();
        }
        OkPacket ok = new OkPacket();
        ok.setPacketId(1);
        ok.setAffectedRows(rowSize);
        ok.write(c);
    }
}
