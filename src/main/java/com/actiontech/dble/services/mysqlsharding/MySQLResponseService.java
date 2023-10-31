package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.BackEndRecycleRunnable;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.response.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.status.LoadDataBatch;
import com.actiontech.dble.services.BackendService;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLBackAuthService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    public void execute(BusinessService service, String sql) {
        boolean changeUser = isChangeUser(service);
        if (changeUser) return;

        StringBuilder synSQL = getSynSql(null, null,
                service.getCharset(), service.getTxIsolation(), service.isAutocommit(), service.getUsrVariables(), service.getSysVariables());
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        synAndDoExecute(synSQL, sql, service.getCharset());
    }

    // only for com_stmt_execute
    public void execute(ByteBuffer buffer) {
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        writeDirectly(buffer);
    }

    // only for com_stmt_execute
    public void execute(byte[] originPacket) {
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }

        writeDirectly(originPacket);
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
                writeDirectly(originPacket);
                return;
            } else if (service.isInLoadData()) {
                if (service.isFirstInLoadData()) {
                    protocolResponseHandler = new LoadDataResponseHandler(this);
                }
            } else if (protocolResponseHandler != defaultResponseHandler) {
                protocolResponseHandler = defaultResponseHandler;
            }
        }

        StringBuilder synSQL = getSynSql(service.getCharset(), service.getTxIsolation(), service.isAutocommit(),
                service.getUsrVariables(), service.getSysVariables());
        if (synSQL != null) {
            sendQueryCmd(synSQL.toString(), service.getCharset());
        }

        isExecuting = true;
        connection.setLastTime(TimeUtil.currentTimeMillis());
        writeDirectly(originPacket);
    }

    //-------------------------------------- for sharding ----------------------------------------------------
    public void execute(RouteResultsetNode rrn, ShardingService service, boolean isAutoCommit) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "execute-route-result");
        TraceManager.log(ImmutableMap.of("route-result-set", rrn, "service-detail", this.compactInfo()), traceObject);
        try {
            if (!service.isAutocommit() && !service.isTxStart() && rrn.isModifySQL()) {
                service.setTxStart(true);
                //Optional.ofNullable(StatisticListener2.getInstance().getRecorder(service)).ifPresent(r -> r.onTxStartByBegin(service));
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
            StringBuilder synSQL = getSynSql(xaTxId, rrn,
                    service.getCharset(), service.getTxIsolation(), isAutoCommit, service.getUsrVariables(), service.getSysVariables());
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
        StringBuilder synSQL = getSynSql(null, rrn, connection.getCharsetName(), this.txIsolation, isAutoCommit, usrVariables, sysVariables);
        if (protocolResponseHandler != defaultResponseHandler) {
            protocolResponseHandler = defaultResponseHandler;
        }
        synAndDoExecute(synSQL, rrn.getStatement(), connection.getCharsetName());
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
            StringBuilder synSQL = getSynSql(xaTxId, rrn, service.getCharset(),
                    service.getTxIsolation(), isAutoCommit, service.getUsrVariables(), service.getSysVariables());
            if (rrn.getSqlType() == ServerParse.LOAD_DATA_INFILE_SQL) {
                protocolResponseHandler = new LoadDataResponseHandler(this);
            } else if (protocolResponseHandler != defaultResponseHandler) {
                protocolResponseHandler = defaultResponseHandler;
            }
            synAndDoExecuteMultiNode(synSQL, rrn, service.getCharset());
        } catch (Exception e) {
            LOGGER.info("route error {},{},{}", rrn, this, service);
            throw e;
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
        this.sendQueryCmd(cmd, connection.getCharsetName());
    }

    private StringBuilder getSynSql(String xaTxID, RouteResultsetNode rrn, CharsetNames clientCharset, int clientTxIsolation,
                                    boolean expectAutocommit, Map<String, String> usrVariables, Map<String, String> sysVariables) {

        StringBuilder sb = getSynSql(clientCharset, clientTxIsolation, expectAutocommit, usrVariables, sysVariables);

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
            packet.setArg(query.getBytes(CharsetUtil.getJavaCharset(clientCharset.getClient())));
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

    @Override
    protected boolean innerRelease() {
        if (isRowDataFlowing) {
            if (logResponse.compareAndSet(false, true)) {
                session.setBackendResponseEndTime(this);
            }
            if (SystemConfig.getInstance().getEnableAsyncRelease() == 1) {
                DbleServer.getInstance().getComplexQueryExecutor().execute(new BackEndRecycleRunnable(this));
                return false;
            } else {
                new BackEndRecycleRunnable(this).run();
                return false;
            }

        }
        complexQuery = false;
        attachment = null;
        statusSync = null;
        isDDL = false;
        testing = false;
        responseHandler = null;
        session = null;
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
            StringBuilder synSQL = getSynSql(xaTxId, rrn, service.getCharset(),
                    service.getTxIsolation(), isAutoCommit, service.getUsrVariables(), service.getSysVariables());
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
            packet.setArg(query.getBytes(CharsetUtil.getJavaCharset(clientCharset.getClient())));
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

    public BackendConnection getConnection() {
        return connection;
    }

    @Override
    protected boolean beforeHandlingTask() {
        if (session != null) {
            if (session.isKilled()) {
                return false;
            }
            session.setBackendResponseTime(this);
        }
        return true;
    }

    @Override
    protected Executor getExecutor() {
        Executor executor;
        if (complexQuery || SystemConfig.getInstance().getUsePerformanceMode() == 1) {
            executor = DbleServer.getInstance().getComplexQueryExecutor();
        } else {
            executor = DbleServer.getInstance().getBackendBusinessExecutor();
        }
        return executor;
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

    public void setSession(NonBlockingSession session) {
        this.session = session;
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
