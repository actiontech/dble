/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.BaseNode;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.ConditionUtil;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * for execute Sql,transform the response data to next handler
 */
public class BaseSelectHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSelectHandler.class);

    private final boolean autocommit;
    private volatile int fieldCounts = -1;

    private final NonBlockingSession serverSession;

    public BaseSelectHandler(long id, RouteResultsetNode rrss, boolean autocommit, Session session) {
        super(id, session, rrss);
        serverSession = (NonBlockingSession) session;
        this.autocommit = autocommit;
    }

    public BackendConnection initConnection() throws Exception {
        if (serverSession.closed()) {
            LOGGER.warn(" FrontendConnection is closed without sending a statement, conn is " + serverSession.getShardingService().getConnection());
            throw new IOException("FrontendConnection is closed");
        }

        BackendConnection exeConn = serverSession.getTarget(rrss);
        if (serverSession.tryExistsCon(exeConn, rrss)) {
            exeConn.getBackendService().setRowDataFlowing(true);
            exeConn.getBackendService().setResponseHandler(this);
            return exeConn;
        } else {
            BaseNode dn = DbleServer.getInstance().getConfig().getAllNodes().get(rrss.getName());
            //autocommit is serverSession.getWriteSource().isAutocommit() && !serverSession.getWriteSource().isTxStart()
            final BackendConnection newConn = dn.getConnection(dn.getDatabase(), autocommit, rrss);
            serverSession.bindConnection(rrss, newConn);
            newConn.getBackendService().setResponseHandler(this);
            newConn.getBackendService().setRowDataFlowing(true);
            return newConn;
        }
    }

    public void execute(MySQLResponseService service) {
        TraceManager.crossThread(service, "base-sql-execute", serverSession.getShardingService());
        if (serverSession.closed()) {
            service.setRowDataFlowing(false);
            serverSession.clearResources(true);
            return;
        }
        service.setSession(serverSession);
        if (service.getConnection().isClosed()) {
            service.setRowDataFlowing(false);
            serverSession.onQueryError("failed or cancelled by other thread".getBytes());
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service.toString() + " send sql:" + rrss.getStatement());
        }
        service.executeMultiNode(rrss, serverSession.getShardingService(), autocommit);
    }


    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("receive ok packet for sync context, service {}", service);
        }
        ((MySQLResponseService) service).syncAndExecute();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        serverSession.setHandlerEnd(this);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service.toString() + "'s field is reached.");
        }
        if (terminate.get()) {
            return;
        }
        if (fieldCounts == -1) {
            fieldCounts = fields.size();
        }
        List<FieldPacket> fieldPackets = new ArrayList<>();

        for (byte[] field1 : fields) {
            FieldPacket field = new FieldPacket();
            field.read(field1);
            if (rrss.isApNode()) {
                handleFieldsOfOLAP(field);
            }
            fieldPackets.add(field);
        }
        nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
    }

    private void handleFieldsOfOLAP(FieldPacket field) {
        String charset = CharsetUtil.getJavaCharset(field.getCharsetIndex());
        try {
            String column = new String(field.getName(), charset);
            int separatorIndex = column.indexOf(StringUtil.TABLE_COLUMN_SEPARATOR);
            if (rrss.getTableAliasMap().isEmpty() || rrss.getTableSet().isEmpty()) {
                throw new RuntimeException("parse error: table name should not be empty.");
            }
            if (separatorIndex < 0) {
                //first table-clickhouse rules
                Map.Entry<String, String> firstTable = rrss.getTableAliasMap().entrySet().iterator().next();
                populateTableInfo(field, column, firstTable, charset);
            } else {
                //Only clickhouse will return table.column format
                String tableName = column.substring(0, separatorIndex);
                String columnName = column.substring(++separatorIndex);
                //first: key:alias value:tableName
                //second: key:tableName value:tableName
                Optional<Map.Entry<String, String>> tableOptional = rrss.getTableAliasMap().entrySet().stream().filter(entity -> StringUtil.equals(entity.getKey(), tableName)).findFirst();
                tableOptional.ifPresent(table -> populateTableInfo(field, columnName, table, charset));

            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("parser error ,charset :" + charset);
        }
    }

    private void populateTableInfo(FieldPacket field, String columnName, Map.Entry<String, String> table, String charset) {
        try {
            String tableAlias = StringUtil.removeBackQuote(table.getKey());
            String tableName = StringUtil.removeBackQuote(table.getValue());
            int tableIndex;
            if ((tableIndex = tableName.indexOf(StringUtil.TABLE_COLUMN_SEPARATOR)) >= 0) {
                tableName = tableName.substring(++tableIndex);
            }
            field.setName(columnName.getBytes(charset));
            field.setTable(tableAlias.getBytes(charset));
            field.setOrgTable(tableName.getBytes(charset));

            for (String tableFullName : rrss.getTableSet()) {
                //key:schemaName value:tableName
                Pair<String, String> tableInfo = ConditionUtil.getTableInfo(rrss.getTableAliasMap(), tableFullName, null);
                if (StringUtil.equals(tableName, tableInfo.getValue())) {
                    if (!DbleServer.getInstance().getConfig().getSchemas().containsKey(tableInfo.getKey())) {
                        throw new RuntimeException("schema not found:" + tableInfo.getKey());
                    }
                    String defaultApNode = DbleServer.getInstance().getConfig().getSchemas().get(tableInfo.getKey()).getDefaultApNode();
                    String database = DbleServer.getInstance().getConfig().getApNodes().get(defaultApNode).getDatabase();
                    field.setDb(database.getBytes(charset));
                    break;
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("parser error ,charset :" + charset);
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService conn) {
        if (terminate.get())
            return true;
        RowDataPacket rp = new RowDataPacket(fieldCounts);
        rp.read(row);
        nextHandler.rowResponse(null, rp, this.isLeft, conn);
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service + " 's rowEof is reached.");
        }
        if (this.terminate.get()) {
            return;
        }
        nextHandler.rowEofResponse(data, this.isLeft, service);
    }

    /**
     * 1. if some connection's thread status is await. 2. if some connection's
     * thread status is running.
     */
    @Override
    public void connectionError(Throwable e, Object attachment) {
        if (terminate.get())
            return;
        String errMsg;
        if (e instanceof MySQLOutPutException) {
            errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
        } else if (e instanceof NullPointerException) {
            errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
        } else {
            RouteResultsetNode node = (RouteResultsetNode) attachment;
            errMsg = "can't connect to shardingNode[" + node.getName() + "],due to " + e.getMessage();
        }
        LOGGER.warn(errMsg, e);
        serverSession.onQueryError(errMsg.getBytes());
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        if (terminate.get())
            return;
        LOGGER.warn(service.toString() + "|connectionClose()|" + reason);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((BackendConnection) service.getConnection()).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        serverSession.onQueryError(reason.getBytes());
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg;
        try {
            errMsg = new String(errPacket.getMessage(), CharsetUtil.getJavaCharset(service.getCharset().getResults()));
        } catch (UnsupportedEncodingException e) {
            errMsg = "UnsupportedEncodingException:" + service.getCharset();
        }
        LOGGER.info(service.toString() + errMsg);
        if (terminate.get())
            return;
        serverSession.onQueryError(errMsg.getBytes());
    }

    @Override
    protected void onTerminate() {
        if (autocommit && !serverSession.getShardingService().isLockTable()) {
            this.serverSession.releaseConnection(rrss, false);
        } else {
            //the connection should wait until the connection running finish
            this.serverSession.waitFinishConnection(rrss);
        }
    }

    @Override
    public HandlerType type() {
        return HandlerType.BASESEL;
    }

}
