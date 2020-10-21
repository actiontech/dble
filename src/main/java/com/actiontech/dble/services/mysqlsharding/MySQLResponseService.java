package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.handler.BackEndCleaner;
import com.actiontech.dble.net.handler.BackEndRecycleRunnable;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.VariablesService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLBackAuthService;
import com.actiontech.dble.services.rwsplit.MysqlPrepareLogicHandler;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.actiontech.dble.util.exception.UnknownTxIsolationException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLResponseService extends VariablesService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLResponseService.class);

    private ResponseHandler responseHandler;

    protected final AtomicBoolean isHandling = new AtomicBoolean(false);

    private volatile boolean isExecuting = false;

    private volatile Object attachment;

    private volatile NonBlockingSession session;

    private volatile boolean metaDataSynced = true;

    private final AtomicBoolean logResponse = new AtomicBoolean(false);
    private volatile boolean complexQuery;
    private volatile boolean isDDL = false;
    private volatile boolean prepareOK = false;
    private volatile boolean testing = false;
    private volatile StatusSync statusSync;
    private volatile boolean isRowDataFlowing = false;
    private volatile BackEndCleaner recycler = null;
    private volatile TxState xaStatus = TxState.TX_INITIALIZE_STATE;
    private boolean autocommitSynced;
    private boolean isolationSynced;
    private volatile String dbuser;

    private MysqlBackendLogicHandler baseLogicHandler;
    private MysqlPrepareLogicHandler prepareLogicHandler;

    private static final CommandPacket COMMIT = new CommandPacket();
    private static final CommandPacket ROLLBACK = new CommandPacket();

    protected BackendConnection connection;

    static {
        COMMIT.setPacketId(0);
        COMMIT.setCommand(MySQLPacket.COM_QUERY);
        COMMIT.setArg("commit".getBytes());
        ROLLBACK.setPacketId(0);
        ROLLBACK.setCommand(MySQLPacket.COM_QUERY);
        ROLLBACK.setArg("rollback".getBytes());
    }

    public MySQLResponseService(AbstractConnection connection) {
        super(connection);
        this.connection = (BackendConnection) connection;
        initFromConfig();
        this.proto = new MySQLProtoHandlerImpl();
        this.baseLogicHandler = new MysqlBackendLogicHandler(this);
        this.prepareLogicHandler = new MysqlPrepareLogicHandler(this);
    }


    private void initFromConfig() {
        this.autocommitSynced = connection.getInstance().isAutocommitSynced();
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        this.isolationSynced = connection.getInstance().isIsolationSynced();
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            this.txIsolation = -1;
        }
        this.complexQuery = false;
        this.usrVariables = new LinkedHashMap<>();
        this.sysVariables = new LinkedHashMap<>();
        this.dbuser = connection.getInstance().getConfig().getUser();
    }


    @Override
    public void handleData(ServiceTask task) {
        if (isSupportCompress()) {
            List<byte[]> packs = CompressUtil.decompressMysqlPacket(task.getOrgData(), new ConcurrentLinkedQueue<byte[]>());
            for (byte[] pack : packs) {
                if (pack.length != 0) {
                    handleInnerData(pack);
                }
            }
        } else {
            this.handleInnerData(task.getOrgData());
        }
    }


    @Override
    protected void handleInnerData(byte[] data) {
        try {
            if (connection.isClosed()) {
                return;
            }
            if (prepareOK) {
                prepareLogicHandler.handleInnerData(data);
            } else {
                baseLogicHandler.handleInnerData(data);
            }

        } finally {
            synchronized (this) {
                currentTask = null;
            }
        }
    }

    protected void handleInnerData() {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "loop-handle-back-data");
        try {
            ServiceTask task;
            //LOGGER.info("LOOP FOR BACKEND " + Thread.currentThread().getName() + " " + taskQueue.size());
            //threadUsageStat start
            String threadName = null;
            ThreadWorkUsage workUsage = null;
            long workStart = 0;
            if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
                threadName = Thread.currentThread().getName();
                workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);
                if (threadName.startsWith("backend")) {
                    if (workUsage == null) {
                        workUsage = new ThreadWorkUsage();
                        DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                    }
                }

                workStart = System.nanoTime();
            }
            //handleData
            while ((task = taskQueue.poll()) != null) {
                handleData(task);
            }
            //threadUsageStat end
            if (workUsage != null && threadName.startsWith("backend")) {
                workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
            }
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
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
    public void taskToTotalQueue(ServiceTask task) {
        Executor executor;
        if (this.isComplexQuery()) {
            executor = DbleServer.getInstance().getComplexQueryExecutor();
        } else {
            executor = DbleServer.getInstance().getBackendBusinessExecutor();
        }

        if (isHandling.compareAndSet(false, true)) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleInnerData();
                    } catch (Exception e) {
                        handleDataError(e);
                    } finally {
                        isHandling.set(false);
                        if (taskQueue.size() > 0) {
                            taskToTotalQueue(null);
                        }
                    }
                }
            });
        }
    }

    protected void handleDataError(Exception e) {
        LOGGER.info(this.toString() + " handle data error:", e);
        while (taskQueue.size() > 0) {
            taskQueue.clear();
            // clear all data from the client
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        }
        if (prepareOK) {
            prepareLogicHandler.reset();
        } else {
            baseLogicHandler.reset();
        }
        connection.close("handle data error:" + e.getMessage());
    }


    public void ping() {
        this.writeDirectly(PingPacket.PING);
    }

    public void execCmd(String cmd) {
        this.sendQueryCmd(cmd, this.getConnection().getCharsetName());
    }

    public void sendQueryCmd(String query, CharsetNames clientCharset) {
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


    public String getConnXID(String sessionXaId, long multiplexNum) {
        if (sessionXaId == null)
            return null;
        else {
            String strMultiplexNum = multiplexNum == 0 ? "" : "." + multiplexNum;
            return sessionXaId.substring(0, sessionXaId.length() - 1) + "." + connection.getSchema() + strMultiplexNum + "'";
        }
    }


    public boolean syncAndExecute() {
        StatusSync sync = this.statusSync;
        if (sync == null) {
            isExecuting = false;
            return true;
        } else {
            boolean executed = sync.synAndExecuted(this);
            if (executed) {
                isExecuting = false;
                statusSync = null;
            }
            return executed;
        }
    }

    public void query(String query) {
        query(query, this.autocommit);
    }

    public void query(String query, boolean isAutoCommit) {
        RouteResultsetNode rrn = new RouteResultsetNode("default", ServerParse.SELECT, query);
        StringBuilder synSQL = getSynSql(null, rrn, this.getConnection().getCharsetName(), this.txIsolation, isAutoCommit, usrVariables, sysVariables);
        synAndDoExecute(synSQL, rrn.getStatement(), this.getConnection().getCharsetName());
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
                    session.setBackendRequestTime(this.getConnection().getId());
                }
                sendQueryCmd(sql, clientCharset);
                return;
            }

            // and our query sql to multi command at last
            synSQL.append(sql).append(";");
            // syn and execute others
            if (session != null) {
                session.setBackendRequestTime(this.getConnection().getId());
            }
            this.sendQueryCmd(synSQL.toString(), clientCharset);
            // waiting syn result...
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    private StringBuilder getSynSql(String xaTxID, RouteResultsetNode rrn, CharsetNames clientCharset, int clientTxIsolation,
                                    boolean expectAutocommit, Map<String, String> usrVariables, Map<String, String> sysVariables) {

        int xaSyn = 0;
        if (!expectAutocommit && xaTxID != null && xaStatus == TxState.TX_INITIALIZE_STATE && !isDDL) {
            // clientTxIsolation = Isolation.SERIALIZABLE;TODO:NEEDED?
            xaSyn = 1;
        }

        Set<String> toResetSys = new HashSet<>();
        String setSql = getSetSQL(usrVariables, sysVariables, toResetSys);
        int setSqlFlag = setSql == null ? 0 : 1;
        int schemaSyn = StringUtil.equals(connection.getSchema(), connection.getOldSchema()) || connection.getSchema() == null ? 0 : 1;
        int charsetSyn = this.getConnection().getCharsetName().equals(clientCharset) ? 0 : 1;
        int txIsolationSyn = (this.txIsolation == clientTxIsolation) ? 0 : 1;
        int autoCommitSyn = (this.autocommit == expectAutocommit) ? 0 : 1;
        int synCount = schemaSyn + charsetSyn + txIsolationSyn + autoCommitSyn + xaSyn + setSqlFlag;
        if (synCount == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        if (schemaSyn == 1) {
            getChangeSchemaCommand(sb, connection.getSchema());
        }
        if (charsetSyn == 1) {
            getCharsetCommand(sb, clientCharset);
        }
        if (txIsolationSyn == 1) {
            getTxIsolationCommand(sb, clientTxIsolation);
        }
        if (autoCommitSyn == 1) {
            getAutocommitCommand(sb, expectAutocommit);
        }
        if (setSqlFlag == 1) {
            sb.append(setSql);
        }
        if (xaSyn == 1) {
            XaDelayProvider.delayBeforeXaStart(rrn.getName(), xaTxID);
            sb.append("XA START ").append(xaTxID).append(";");
            this.xaStatus = TxState.TX_STARTED_STATE;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con need syn, total syn cmd " + synCount +
                    " commands " + sb.toString() + ",schema change:" +
                    (schemaSyn == 1) + ", con:" + this);
        }
        metaDataSynced = false;
        statusSync = new StatusSync(connection.getSchema(),
                clientCharset, clientTxIsolation, expectAutocommit,
                synCount, usrVariables, sysVariables, toResetSys);
        return sb;
    }

    private static void getChangeSchemaCommand(StringBuilder sb, String schema) {
        if (schema != null) {
            sb.append("use `");
            sb.append(schema);
            sb.append("`;");
        }
    }

    private static void getCharsetCommand(StringBuilder sb, CharsetNames clientCharset) {
        sb.append("SET CHARACTER_SET_CLIENT = ");
        sb.append(clientCharset.getClient());
        sb.append(",CHARACTER_SET_RESULTS = ");
        sb.append(clientCharset.getResults());
        sb.append(",COLLATION_CONNECTION = ");
        sb.append(clientCharset.getCollation());
        sb.append(";");
    }

    private static void getTxIsolationCommand(StringBuilder sb, int txIsolation) {
        switch (txIsolation) {
            case Isolations.READ_UNCOMMITTED:
                sb.append("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
                return;
            case Isolations.READ_COMMITTED:
                sb.append("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;");
                return;
            case Isolations.REPEATABLE_READ:
                sb.append("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
                return;
            case Isolations.SERIALIZABLE:
                sb.append("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
                return;
            default:
                throw new UnknownTxIsolationException("txIsolation:" + txIsolation);
        }
    }

    private void getAutocommitCommand(StringBuilder sb, boolean autoCommit) {
        if (autoCommit) {
            sb.append("SET autocommit=1;");
        } else {
            sb.append("SET autocommit=0;");
        }
    }

    private String getSetSQL(Map<String, String> usrVars, Map<String, String> sysVars, Set<String> toResetSys) {
        //new final var
        List<Pair<String, String>> setVars = new ArrayList<>();
        //tmp add all backend sysVariables
        Map<String, String> tmpSysVars = new HashMap<>(sysVariables);
        //for all front end sysVariables
        for (Map.Entry<String, String> entry : sysVars.entrySet()) {
            if (!tmpSysVars.containsKey(entry.getKey())) {
                setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
            } else {
                String value = tmpSysVars.remove(entry.getKey());
                //if backend is not equal frontend, need to reset
                if (!StringUtil.equalsIgnoreCase(entry.getValue(), value)) {
                    setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
            }
        }
        //tmp now = backend -(backend &&frontend)
        for (Map.Entry<String, String> entry : tmpSysVars.entrySet()) {
            String value = DbleServer.getInstance().getSystemVariables().getDefaultValue(entry.getKey());
            try {
                BigDecimal vl = new BigDecimal(value);
            } catch (NumberFormatException e) {
                value = "`" + value + "`";
            }
            setVars.add(new Pair<>(entry.getKey(), value));
            toResetSys.add(entry.getKey());
        }

        for (Map.Entry<String, String> entry : usrVars.entrySet()) {
            if (!usrVariables.containsKey(entry.getKey())) {
                setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
            } else {
                if (!StringUtil.equalsIgnoreCase(entry.getValue(), usrVariables.get(entry.getKey()))) {
                    setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
            }
        }

        if (setVars.size() == 0)
            return null;
        StringBuilder sb = new StringBuilder("set ");
        int cnt = 0;
        for (Pair<String, String> var : setVars) {
            if (cnt > 0) {
                sb.append(",");
            }
            sb.append(var.getKey());
            sb.append("=");
            sb.append(var.getValue());
            cnt++;
        }
        sb.append(";");
        return sb.toString();
    }


    public void release() {
        if (!metaDataSynced) { // indicate connection not normal finished
            LOGGER.info("can't sure connection syn result,so close it " + this);
            this.responseHandler = null;
            this.connection.businessClose("syn status unknown ");
            return;
        }

        if (this.usrVariables.size() > 0) {
            this.responseHandler = null;
            this.connection.businessClose("close for clear usrVariables");
            return;
        }
        if (isRowDataFlowing) {
            if (logResponse.compareAndSet(false, true)) {
                session.setBackendResponseEndTime(this);
            }
            DbleServer.getInstance().getComplexQueryExecutor().execute(new BackEndRecycleRunnable(this));
            return;
        }

        complexQuery = false;
        metaDataSynced = true;
        attachment = null;
        statusSync = null;
        isDDL = false;
        testing = false;
        setResponseHandler(null);
        setSession(null);
        logResponse.set(false);
        TraceManager.sessionFinish(this);
        ((PooledConnection) connection).getPoolRelated().release((PooledConnection) connection);
    }

    public void onConnectionClose(String reason) {
        final ResponseHandler handler = responseHandler;
        final MySQLResponseService responseService = this;
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    responseService.backendSpecialCleanUp();
                    if (handler != null) {
                        handler.connectionClose(responseService, reason);
                    }
                } catch (Throwable e) {
                    LOGGER.warn("get error close mysql connection ", e);
                }
            }
        });
    }

    @Override
    public void cleanup() {
        super.cleanup();
        backendSpecialCleanUp();
    }

    public void backendSpecialCleanUp() {
        this.setExecuting(false);
        this.setRowDataFlowing(false);
        this.signal();
    }

    public String compactInfo() {
        return "MySQLConnection host=" + connection.getHost() + ", port=" + connection.getPort() + ", schema=" + connection.getSchema();
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
            synAndDoExecuteMultiNode(synSQL, rrn, service.getCharset());
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    public void execute(RouteResultsetNode rrn, ShardingService service, boolean isAutoCommit) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "execute-route-result");
        TraceManager.log(ImmutableMap.of("route-result-set", rrn, "service-detail", this.compactInfo()), traceObject);
        try {
            String xaTxId = getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
            if (!service.isAutocommit() && !service.isTxStart() && rrn.isModifySQL()) {
                service.setTxStart(true);
            }
            if (rrn.getSqlType() == ServerParse.DDL) {
                isDDL = true;
            }
            StringBuilder synSQL = getSynSql(xaTxId, rrn,
                    service.getCharset(), service.getTxIsolation(), isAutoCommit, service.getUsrVariables(), service.getSysVariables());
            synAndDoExecute(synSQL, rrn.getStatement(), service.getCharset());
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    public void execute(BusinessService service, String sql) {
        if (connection.getSchema() == null && connection.getOldSchema() != null) {
            // change user
            changeUser();
        } else {
            StringBuilder synSQL = getSynSql(null, null,
                    service.getCharset(), service.getTxIsolation(), service.isAutocommit(), service.getUsrVariables(), service.getSysVariables());
            synAndDoExecute(synSQL, sql, service.getCharset());
        }
    }

    public void execute(RWSplitService service, byte[] originPacket) {
        if (service.getSchema() == null && getSchema() != null) {
            // change user
            changeUser();
        } else {
            StringBuilder synSQL = getSynSql(null, null,
                    service.getCharset(), service.getTxIsolation(), service.isAutocommit(), service.getUsrVariables(), service.getSysVariables());
            if (synSQL != null) {
                sendQueryCmd(synSQL.toString(), service.getCharset());
            }

            prepareOK = originPacket[4] == MySQLPacket.COM_STMT_PREPARE;
            writeDirectly(originPacket);
        }
    }

    //  the purpose is to set old schema to null
    private void changeUser() {
        DbInstanceConfig config = connection.getInstance().getConfig();
        connection.setService(new MySQLBackAuthService(connection, config.getUser(), config.getPassword(), connection.getBackendService().getResponseHandler()));
        ChangeUserPacket changeUserPacket = new ChangeUserPacket(config.getUser());
        changeUserPacket.setCharsetIndex(CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset()));
        changeUserPacket.bufferWrite(connection);
    }

    private void synAndDoExecuteMultiNode(StringBuilder synSQL, RouteResultsetNode rrn, CharsetNames clientCharset) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send cmd by WriteToBackendExecutor to conn[" + this + "]");
        }

        if (synSQL == null) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this.getConnection().getId());
            }
            DbleServer.getInstance().getWriteToBackendQueue().add(Collections.singletonList(sendQueryCmdTask(rrn.getStatement(), clientCharset)));
            return;
        }
        // syn sharding
        List<WriteToBackendTask> taskList = new ArrayList<>(1);
        // and our query sql to multi command at last
        synSQL.append(rrn.getStatement()).append(";");
        // syn and execute others
        if (session != null) {
            session.setBackendRequestTime(this.getConnection().getId());
        }
        taskList.add(sendQueryCmdTask(synSQL.toString(), clientCharset));
        DbleServer.getInstance().getWriteToBackendQueue().add(taskList);
        // waiting syn result...

    }

    public void resetContextStatus() {
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            this.txIsolation = -1;
        }
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        this.connection.initCharacterSet(SystemConfig.getInstance().getCharset());
        this.usrVariables.clear();
        this.sysVariables.clear();
        this.sysVariables.put("sql_mode", null);
    }

    private WriteToBackendTask sendQueryCmdTask(String query, CharsetNames clientCharset) {
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

    public void signal() {
        if (recycler != null) {
            recycler.signal();
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

    public void setResponseHandler(ResponseHandler handler) {
        this.responseHandler = handler;
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

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public boolean isRowDataFlowing() {
        return isRowDataFlowing;
    }

    public void setRowDataFlowing(boolean rowDataFlowing) {
        isRowDataFlowing = rowDataFlowing;
    }

    public BackEndCleaner getRecycler() {
        return recycler;
    }

    public AtomicBoolean getLogResponse() {
        return logResponse;
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

    public StatusSync getStatusSync() {
        return statusSync;
    }

    public void setStatusSync(StatusSync statusSync) {
        this.statusSync = statusSync;
    }

    public void setRecycler(BackEndCleaner recycler) {
        this.recycler = recycler;
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


    public boolean isExecuting() {
        return isExecuting;
    }

    public void setExecuting(boolean executing) {
        isExecuting = executing;
    }

    public String toString() {
        return "MySQLResponseService[isExecuting = " + isExecuting + " attachment = " + attachment + " autocommitSynced = " + autocommitSynced + " isolationSynced = " + isolationSynced +
                " xaStatus = " + xaStatus + " isDDL = " + isDDL + " complexQuery = " + complexQuery + "] with response handler [" + responseHandler + "] with rrs = [" +
                attachment + "]  with connection " + connection.toString();
    }

    private static class StatusSync {
        private final String schema;
        private final CharsetNames clientCharset;
        private final Integer txtIsolation;
        private final Boolean autocommit;
        private final AtomicInteger synCmdCount;
        private final Map<String, String> usrVariables = new LinkedHashMap<>();
        private final Map<String, String> sysVariables = new LinkedHashMap<>();

        StatusSync(String schema,
                   CharsetNames clientCharset, Integer txtIsolation, Boolean autocommit,
                   int synCount, Map<String, String> usrVariables, Map<String, String> sysVariables, Set<String> toResetSys) {
            this.schema = schema;
            this.clientCharset = clientCharset;
            this.txtIsolation = txtIsolation;
            this.autocommit = autocommit;
            this.synCmdCount = new AtomicInteger(synCount);
            this.usrVariables.putAll(usrVariables);
            this.sysVariables.putAll(sysVariables);
            for (String sysVariable : toResetSys) {
                this.sysVariables.remove(sysVariable);
            }
        }

        boolean synAndExecuted(MySQLResponseService service) {
            int remains = synCmdCount.decrementAndGet();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("synAndExecuted " + remains + ",conn info:" + service);
            }
            if (remains == 0) { // syn command finished
                this.updateConnectionInfo(service);
                service.metaDataSynced = true;
                return false;
            }
            return remains < 0;
        }

        private void updateConnectionInfo(MySQLResponseService service) {
            if (schema != null) {
                service.connection.setSchema(schema);
                service.connection.setOldSchema(schema);
            }
            if (clientCharset != null) {
                service.connection.setCharsetName(clientCharset);
            }
            if (txtIsolation != null) {
                service.txIsolation = txtIsolation;
            }
            if (autocommit != null) {
                service.autocommit = autocommit;
            }
            service.sysVariables = sysVariables;
            service.usrVariables = usrVariables;
        }
    }
}
