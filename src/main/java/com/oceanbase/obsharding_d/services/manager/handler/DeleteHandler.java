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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DeleteHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteHandler.class);

    public void handle(String stmt, ManagerService service) {
        MySqlDeleteStatement delete;
        try {
            delete = (MySqlDeleteStatement) (DruidUtil.parseMultiSQL(stmt));
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
        if (managerBaseTable == null || !managerBaseTable.isWritable()) {
            service.writeErrMessage("42000", "Access denied for table '" + schemaInfo.getTable() + "'", ErrorCode.ER_ACCESS_DENIED_ERROR);
            return;
        }
        PacketResult packetResult = new PacketResult();
        ManagerWritableTable managerTable = (ManagerWritableTable) managerBaseTable;
        if (ClusterConfig.getInstance().isClusterEnable()) {
            deleteWithCluster(service, delete, managerTable, packetResult);
        } else {
            generalDelete(service, delete, managerTable, packetResult);
        }
        writePacket(packetResult.isSuccess(), packetResult.getRowSize(), service, packetResult.getErrorMsg());
    }

    private void deleteWithCluster(ManagerService service, MySqlDeleteStatement delete, ManagerWritableTable managerTable, PacketResult packetResult) {
        //cluster-lock
        DistributeLock distributeLock;
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        distributeLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getConfChangeLockPath());
        if (!distributeLock.acquire()) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Other instance is reloading, please try again later.");
            return;
        }
        LOGGER.info("delete OBsharding-D_information[{}]: added distributeLock {}", managerTable.getTableName(), ClusterMetaUtil.getConfChangeLockPath());
        try {
            generalDelete(service, delete, managerTable, packetResult);
        } finally {
            distributeLock.release();
        }
    }

    private void generalDelete(ManagerService service, MySqlDeleteStatement delete, ManagerWritableTable managerTable, PacketResult packetResult) {
        //stand-alone lock
        int rowSize;
        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        boolean lockFlag = lock.writeLock().tryLock();
        if (!lockFlag) {
            service.writeErrMessage(ErrorCode.ER_YES, "Other threads are executing reload config or management commands(insert/update/delete), please try again later.");
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
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
        } catch (ReloadException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg(e.getMessage());
        } catch (ConfigException e) {
            packetResult.setSuccess(false);
            packetResult.setErrorMsg("Delete failure.The reason is " + e.getMessage());
        } catch (Exception e) {
            packetResult.setSuccess(false);
            if (e.getCause() instanceof ReloadException) {
                packetResult.setErrorMsg("Delete failure.The reason is " + e.getMessage());
                packetResult.setErrorCode(((ReloadException) e).getErrorCode());
            } else if (e.getCause() instanceof ConfigException) {
                packetResult.setErrorMsg("Delete failure.The reason is " + e.getMessage());
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
}
