/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.BackEndRecycleRunnable;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.response.*;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.net.service.WriteFlags;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.status.LoadDataBatch;
import com.actiontech.dble.services.BackendService;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.VariablesService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLBackAuthService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLResponseService extends BackendService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLResponseService.class);

    private volatile ResponseHandler responseHandler;
    private volatile Object attachment;
    private volatile NonBlockingSession session;
    private volatile RWSplitNonBlockingSession session2;
    private volatile boolean complexQuery = false;
    private volatile boolean isDDL = false;
    private volatile boolean testing = false;
    private volatile TxState xaStatus = TxState.TX_INITIALIZE_STATE;

    private final AtomicBoolean logResponse = new AtomicBoolean(false);
    private static final CommandPacket COMMIT = new CommandPacket();
    private static final CommandPacket ROLLBACK = new CommandPacket();

    static {
        COMMIT.setPacketId(0);
        COMMIT.setCommand(MySQLPacket.COM_QUERY);
        COMMIT.setArg("commit".getBytes());
        ROLLBACK.setPacketId(0);
        ROLLBACK.setCommand(MySQLPacket.COM_QUERY);
        ROLLBACK.setArg("rollback".getBytes());
    }

    public MySQLResponseService(BackendConnection connection) {
        super(connection);
        this.defaultResponseHandler = new DefaultResponseHandler(this);
        this.protocolResponseHandler = defaultResponseHandler;
    }

    public String getConnXID(String sessionXaId, long multiplexNum) {
        if (sessionXaId == null)
            return null;
        else {
            String strMultiplexNum = multiplexNum == 0 ? "" : "." + multiplexNum;
            return sessionXaId.substring(0, sessionXaId.length() - 1) + "." + connection.getSchema() + strMultiplexNum + "'";
        }
    }

    //-------------------------------------- for rw ----------------------------------------------------
    //  the purpose is to set old schema to null
    private void changeUser() {
        DbInstanceConfig config = connection.getInstance().getConfig();
        connection.setService(new MySQLBackAuthService(connection, config.getUser(), config.getPassword(), connection.getBackendService().getResponseHandler()));
        ChangeUserPacket changeUserPacket = new ChangeUserPacket(config.getUser());
        changeUserPacket.setCharsetIndex(CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset()));
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        changeUserPacket.bufferWrite(connection);
    }

    // only for com_stmt_execute
    public void execute(ByteBuffer buffer) {
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        writeDirectly(buffer, WriteFlags.QUERY_END);
    }

    // only for com_stmt_execute
    public void execute(byte[] originPacket) {
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }

        write(originPacket, WriteFlags.QUERY_END);
    }

    public void execute(BusinessService service, String sql) {
        boolean changeUser = isChangeUser(service);
        if (changeUser) return;

        StringBuilder synSQL = getSynSql(null, null, service.isAutocommit(), service);
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        synAndDoExecute(synSQL, sql, service.getCharset());
    }

    public void execute(RWSplitService service, byte[] originPacket) {
        boolean changeUser = isChangeUser(service);
        if (changeUser) return;

        if (originPacket.length > 4) {
            byte type = originPacket[4];
            if (type == MySQLPacket.COM_STMT_PREPARE) {
                protocolResponseHandler = new PrepareResponseHandler(this);
            } else if (type == MySQLPacket.COM_STMT_EXECUTE) {
                protocolResponseHandler = new ExecuteResponseHandler(this, originPacket[9] == (byte) 0x01);
            } else if (type == MySQLPacket.COM_STMT_FETCH) {
                protocolResponseHandler = new FetchResponseHandler(this);
            } else if (type == MySQLPacket.COM_FIELD_LIST) {
                protocolResponseHandler = new FieldListResponseHandler(this);
            } else if (type == MySQLPacket.COM_STATISTICS) {
                protocolResponseHandler = new StatisticsResponseHandler(this);
            } else if (type == MySQLPacket.COM_STMT_CLOSE) {
                // no response
                write(originPacket, WriteFlags.QUERY_END);
                return;
            } else if (service.isInLoadData()) {
                if (service.isFirstInLoadData()) {
                    protocolResponseHandler = new LoadDataResponseHandler(this);
                }
            } else if (protocolResponseHandler != defaultResponseHandler) {
                protocolResponseHandler = defaultResponseHandler;
            }
        }

        StringBuilder synSQL = getSynSql(service.isAutocommit(), service);
        if (synSQL != null) {
            sendQueryCmd(synSQL.toString(), service.getCharset());
        }
        execCmd(originPacket);

    }

    //-------------------------------------- for sharding ----------------------------------------------------
    public void execute(RouteResultsetNode rrn, ShardingService service, boolean isAutoCommit) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "execute-route-result");
        TraceManager.log(ImmutableMap.of("route-result-set", rrn, "service-detail", this.compactInfo()), traceObject);
        try {
            if (!service.isAutocommit() && !service.isTxStart() && rrn.isModifySQL()) {
                service.setTxStart(true);
                //Optional.ofNullable(StatisticListener2.getInstance().getRecorder(service, r ->r.onTxStartByBegin(service));
            }
            if (rrn.getSqlType() == ServerParse.DDL) {
                isDDL = true;
            }
            if (rrn.getSqlType() == ServerParse.LOAD_DATA_INFILE_SQL) {
                protocolResponseHandler = new LoadDataResponseHandler(this);
            } else if (protocolResponseHandler != defaultResponseHandler) {
                protocolResponseHandler = defaultResponseHandler;
            }
            String xaTxId = getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            StringBuilder synSQL = getSynSql(xaTxId, rrn, isAutoCommit, service);
            synAndDoExecute(synSQL, rrn.getStatement(), service.getCharset());
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    public boolean isChangeUser(BusinessService service) {
        if ((service.getSchema() == null && connection.getSchema() != null) || (service.getSchema() == null && connection.getOldSchema() != null)) {
            // change user
            changeUser();
            return true;
        }
        return false;
    }

    public void query(String query) {
        query(query, this.autocommit);
    }

    public void query(String query, boolean isAutoCommit) {
        RouteResultsetNode rrn = new RouteResultsetNode("default", ServerParse.SELECT, query);
        StringBuilder synSQL = getSynSql(null, rrn, isAutoCommit, this);
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        synAndDoExecute(synSQL, rrn.getStatement(), charsetName);
    }

    public void executeMultiNode(RouteResultsetNode rrn, ShardingService service, boolean isAutoCommit) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "execute-route-multi-result");
        TraceManager.log(ImmutableMap.of("route-result-set", rrn.toString(), "service-detail", this.toString()), traceObject);
        try {
            String xaTxId = getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (!service.isAutocommit() && !service.isTxStart() && rrn.isModifySQL()) {
                service.setTxStart(true);
            }
            if (rrn.getSqlType() == ServerParse.DDL) {
                isDDL = true;
            }
            StringBuilder synSQL = getSynSql(xaTxId, rrn, isAutoCommit, service);
            if (rrn.getSqlType() == ServerParse.LOAD_DATA_INFILE_SQL) {
                protocolResponseHandler = new LoadDataResponseHandler(this);
            } else if (protocolResponseHandler != defaultResponseHandler) {
                protocolResponseHandler = defaultResponseHandler;
            }
            synAndDoExecuteMultiNode(synSQL, rrn, service.getCharset());
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    private void synAndDoExecuteMultiNode(StringBuilder synSQL, RouteResultsetNode rrn, CharsetNames clientCharset) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send cmd by WriteToBackendExecutor to conn[" + this + "]");
        }

        if (synSQL == null) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this);
            }
            DbleServer.getInstance().getWriteToBackendQueue().add(Collections.singletonList(sendQueryCmdTask(rrn.getStatement(), clientCharset)));
            return;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con need syn,sync sql is " + synSQL.toString() + ", con:" + this);
        }
        // and our query sql to multi command at last
        synSQL.append(rrn.getStatement()).append(";");
        // syn and execute others
        if (session != null) {
            session.setBackendRequestTime(this);
        }
        // syn sharding
        List<WriteToBackendTask> taskList = new ArrayList<>(1);
        taskList.add(sendQueryCmdTask(synSQL.toString(), clientCharset));
        DbleServer.getInstance().getWriteToBackendQueue().add(taskList);
        // waiting syn result...
    }

    private void synAndDoExecute(StringBuilder synSQL, String sql, CharsetNames clientCharset) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "syn&do-execute-sql");
        if (synSQL != null && traceObject != null) {
            TraceManager.log(ImmutableMap.of("synSQL", synSQL), traceObject);
        }
        try {
            if (synSQL == null) {
                // not need syn connection
                if (session != null) {
                    session.setBackendRequestTime(this);
                }
                sendQueryCmd(sql, clientCharset);
                return;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("con need syn,sync sql is " + synSQL.toString() + ", con:" + this);
            }
            // and our query sql to multi command at last
            synSQL.append(sql).append(";");
            // syn and execute others
            if (session != null) {
                session.setBackendRequestTime(this);
            }
            this.sendQueryCmd(synSQL.toString(), clientCharset);
            // waiting syn result...
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    public void execCmd(String cmd) {
        this.sendQueryCmd(cmd, charsetName);
    }

    public void execCmd(byte[] originPacket) {
        isExecuting = true;
        connection.setLastTime(TimeUtil.currentTimeMillis());
        write(originPacket, WriteFlags.QUERY_END);

    }

    private StringBuilder getSynSql(String xaTxID, RouteResultsetNode rrn, boolean expectAutocommit, VariablesService front) {

        StringBuilder sb = getSynSql(expectAutocommit, front);

        if (!expectAutocommit && xaTxID != null && xaStatus == TxState.TX_INITIALIZE_STATE && !isDDL) {
            // clientTxIsolation = Isolation.SERIALIZABLE;TODO:NEEDED?
            XaDelayProvider.delayBeforeXaStart(rrn.getName(), xaTxID);
            if (sb == null) {
                sb = new StringBuilder(10);
            }
            sb.append("XA START ").append(xaTxID).append(";");
            addSyncContext();
            this.xaStatus = TxState.TX_STARTED_STATE;
        }
        return sb;
    }

    // send query
    public void sendQueryCmd(String query, CharsetNames clientCharset) {
        if (connection.isClosed()) {
            onConnectionClose("connection is closed before sending cmd");
        }
        CommandPacket packet = new CommandPacket();
        packet.setPacketId(0);
        packet.setCommand(MySQLPacket.COM_QUERY);
        try {
            packet.setArg(query.getBytes(getCharset(clientCharset)));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        isExecuting = true;
        connection.setLastTime(TimeUtil.currentTimeMillis());
        int size = packet.calcPacketSize();
        if (size >= MySQLPacket.MAX_PACKET_SIZE) {
            packet.writeBigPackage(this, size);
        } else {
            packet.write(this);
        }
    }

    private String getCharset(CharsetNames clientCharset) {
        String javaCharset = CharsetUtil.getJavaCharset(clientCharset.getClient());
        if (Objects.isNull(session)) {
            return javaCharset;
        }
        if (session.isIsoCharset()) {
            session.setIsoCharset(false);
            return StringUtil.ISO_8859_1;
        } else {
            return javaCharset;
        }
    }

    @Override
    protected boolean innerRelease() {
        if (isRowDataFlowing) {
            if (logResponse.compareAndSet(false, true)) {
                session.setBackendResponseEndTime(this);
            }
            DbleServer.getInstance().getComplexQueryExecutor().execute(new BackEndRecycleRunnable(this));
            return false;
        }
        complexQuery = false;
        attachment = null;
        statusSync = null;
        isDDL = false;
        testing = false;
        setResponseHandler(null);
        setSession(null);
        logResponse.set(false);
        return true;
    }

    public void onConnectionClose(String reason) {
        final ResponseHandler handler = responseHandler;
        final MySQLResponseService responseService = this;
        StatisticListener.getInstance().record(session, r -> r.onBackendSqlEnd(responseService));
        DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
            try {
                responseService.backendSpecialCleanUp();
                if (handler != null) {
                    handler.connectionClose(responseService, reason);
                }
            } catch (Throwable e) {
                LOGGER.warn("get error close mysql connection ", e);
            }
        });
    }

    public String compactInfo() {
        return "MySQLConnection host=" + connection.getHost() + ", port=" + connection.getPort() + ", schema=" + connection.getSchema();
    }

    public void executeMultiNodeForLoadData(RouteResultsetNode rrn, ShardingService service, boolean isAutoCommit) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "execute-route-multi-result");
        TraceManager.log(ImmutableMap.of("route-result-set", rrn.toString(), "service-detail", this.toString()), traceObject);
        try {
            String xaTxId = getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (!service.isAutocommit() && !service.isTxStart() && rrn.isModifySQL()) {
                service.setTxStart(true);
            }
            if (rrn.getSqlType() == ServerParse.DDL) {
                isDDL = true;
            }
            StringBuilder synSQL = getSynSql(xaTxId, rrn, isAutoCommit, service);
            if (rrn.getSqlType() == ServerParse.LOAD_DATA_INFILE_SQL) {
                protocolResponseHandler = new LoadDataResponseHandler(this);
            } else if (protocolResponseHandler != defaultResponseHandler) {
                protocolResponseHandler = defaultResponseHandler;
            }
            synAndDoExecuteMultiNodeForLoadData(synSQL, rrn, service.getCharset());
        } finally {
            TraceManager.finishSpan(this, traceObject);

        }
    }

    private void synAndDoExecuteMultiNodeForLoadData(StringBuilder synSQL, RouteResultsetNode rrn, CharsetNames clientCharset) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send cmd by WriteToBackendExecutor to conn[" + this + "]");
        }
        if (synSQL == null) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this);
            }
            DbleServer.getInstance().getWriteToBackendQueue().add(Collections.singletonList(sendQueryCmdTask(rrn.getStatement(), clientCharset)));
            waitSyncResult(rrn, clientCharset);
            return;
        }
        // syn sharding
        List<WriteToBackendTask> taskList = new ArrayList<>(1);
        // and our query sql to multi command at last
        // syn and execute others
        synSQL.append(rrn.getStatement()).append(";");
        if (session != null) {
            session.setBackendRequestTime(this);
        }
        taskList.add(sendQueryCmdTask(synSQL.toString(), clientCharset));
        DbleServer.getInstance().getWriteToBackendQueue().add(taskList);
        // waiting syn result...
        waitSyncResult(rrn, clientCharset);
    }

    private WriteToBackendTask sendQueryCmdTask(String query, CharsetNames clientCharset) {
        if (connection.isClosed()) {
            onConnectionClose("connection is closed before sending cmd");
        }
        CommandPacket packet = new CommandPacket();
        packet.setPacketId(0);
        packet.setCommand(MySQLPacket.COM_QUERY);
        try {
            packet.setArg(query.getBytes(getCharset(clientCharset)));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        isExecuting = true;
        connection.setLastTime(TimeUtil.currentTimeMillis());
        return new WriteToBackendTask(this, packet);
    }

    private void waitSyncResult(RouteResultsetNode rrn, CharsetNames clientCharset) {
        while (rrn.getLoadDataRrnStatus() == 0 && LoadDataBatch.getInstance().isEnableBatchLoadData()) {
            LockSupport.parkNanos(100);
        }
        if (rrn.getLoadDataRrnStatus() == 2) {
            this.sendQueryCmd("show warnings;", clientCharset);
        }
    }

    public void rollback() {
        ROLLBACK.write(this);
    }

    public void commit() {
        COMMIT.write(this);
    }

    @Override
    public BackendConnection getConnection() {
        return connection;
    }

    @Override
    protected boolean beforeHandlingTask(@NotNull ServiceTask task) {
        if (session != null) {
            if (session.isKilled()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void beforeInsertServiceTask(@NotNull ServiceTask task) {
        super.beforeInsertServiceTask(task);
        if (session != null) {
            session.setBackendResponseTime(this);
        }
    }

    @Override
    protected Executor getExecutor() {
        Executor executor;
        if (complexQuery) {
            executor = DbleServer.getInstance().getComplexQueryExecutor();
        } else {
            executor = DbleServer.getInstance().getBackendExecutor();
        }
        return executor;
    }

    protected boolean isSupportFlowControl() {
        return true;
    }

    public boolean isComplexQuery() {
        return complexQuery;
    }

    public void setComplexQuery(boolean complexQuery) {
        this.complexQuery = complexQuery;
    }

    public boolean isDDL() {
        return isDDL;
    }

    public void setDDL(boolean ddl) {
        isDDL = ddl;
    }

    public boolean isTesting() {
        return testing;
    }

    public void setTesting(boolean testing) {
        this.testing = testing;
    }

    public TxState getXaStatus() {
        return xaStatus;
    }

    public void setXaStatus(TxState xaStatus) {
        this.xaStatus = xaStatus;
    }

    public String getSchema() {
        return connection.getSchema();
    }

    public void setSchema(String schema) {
        this.connection.setSchema(schema);
    }

    public void setResponseHandler(ResponseHandler handler) {
        this.responseHandler = handler;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public RWSplitNonBlockingSession getSession2() {
        return session2;
    }


    public Session getOriginSession() {
        return session == null ? session2 : session;
    }

    public void setSession(Session session) {
        if (session == null) {
            this.session = null;
            this.session2 = null;
            return;
        }
        if (session instanceof NonBlockingSession) {
            this.session = (NonBlockingSession) session;
            this.session2 = null;
        } else if (session instanceof RWSplitNonBlockingSession) {
            this.session2 = (RWSplitNonBlockingSession) session;
            this.session = null;
        } else {
            throw new UnsupportedOperationException("unsupport cast");
        }
    }

    public AtomicBoolean getLogResponse() {
        return logResponse;
    }

    @Override
    public String toString() {
        return "MySQLResponseService[isExecuting = " + isExecuting + " attachment = " + attachment + " autocommitSynced = " + autocommitSynced + " isolationSynced = " + isolationSynced +
                " xaStatus = " + xaStatus + " isDDL = " + isDDL + " complexQuery = " + complexQuery + "] with response handler [" + responseHandler + "] with rrs = [" +
                attachment + "]  with connection " + connection.toString();
    }

}
