/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.AbstractConnection;
import com.actiontech.dble.net.NIOConnector;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.handler.BackEndCleaner;
import com.actiontech.dble.net.handler.BackEndRecycleRunnable;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.util.PasswordAuthPlugin;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.actiontech.dble.util.exception.UnknownTxIsolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mycatc
 */
public class MySQLConnection extends AbstractConnection implements BackendConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnection.class);

    public static final Comparator<BackendConnection> LAST_ACCESS_COMPARABLE;
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

    static {
        LAST_ACCESS_COMPARABLE = new Comparator<BackendConnection>() {
            @Override
            public int compare(final BackendConnection entryOne, final BackendConnection entryTwo) {
                return Long.compare(entryOne.getLastTime(), entryTwo.getLastTime());
            }
        };
    }

    private AtomicInteger state = new AtomicInteger(INITIAL);

    @Override
    public boolean compareAndSet(int expect, int update) {
        return state.compareAndSet(expect, update);
    }

    @Override
    public void lazySet(int update) {
        state.lazySet(update);
    }

    @Override
    public int getState() {
        return state.get();
    }

    private volatile long lastTime;
    private volatile String schema = null;
    private volatile String oldSchema;

    private volatile boolean isDDL = false;
    private volatile boolean isRowDataFlowing = false;
    private volatile boolean isExecuting = false;
    private volatile StatusSync statusSync;
    private volatile boolean metaDataSynced = true;
    private volatile TxState xaStatus = TxState.TX_INITIALIZE_STATE;
    private volatile int txIsolation;
    private volatile boolean autocommit;
    private volatile boolean complexQuery;
    private volatile NonBlockingSession session;
    private long oldTimestamp;
    private final AtomicBoolean logResponse = new AtomicBoolean(false);
    private volatile boolean testing = false;
    private volatile String closeReason = null;
    private volatile BackEndCleaner recycler = null;
    private AtomicBoolean isCreateFail = new AtomicBoolean(false);

    protected long connectionTimeout;

    public AtomicBoolean getIsCreateFail() {
        return isCreateFail;
    }

    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (usingCompress) {
            flag |= Capabilities.CLIENT_COMPRESS;
        }
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }

    private volatile PhysicalDbInstance dbInstance;
    private boolean fromSlaveDB;
    private long threadId;
    private HandshakeV10Packet handshake;
    private volatile boolean isAuthenticated;
    private String user;
    private String password;
    private Object attachment;
    private boolean autocommitSynced;
    private boolean isolationSynced;
    private volatile ResponseHandler respHandler;

    public MySQLConnection(NetworkChannel channel, DbInstanceConfig config, boolean fromSlaveDB, boolean autocommitSynced, boolean isolationSynced) {
        super(channel);
        this.host = config.getIp();
        this.port = config.getPort();
        this.user = config.getUser();
        this.password = config.getPassword();
        this.connectionTimeout = config.getPoolConfig().getConnectionTimeout();
        this.lastTime = TimeUtil.currentTimeMillis();

        this.autocommitSynced = autocommitSynced;
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        this.fromSlaveDB = fromSlaveDB;
        this.isolationSynced = isolationSynced;
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            /* if the txIsolation in bootstrap.cnf is different from the isolation level in MySQL node,
             * it need to sync the status firstly for new idle connection*/
            this.txIsolation = -1;
        }
        this.complexQuery = false;
        this.usrVariables = new LinkedHashMap<>();
        this.sysVariables = new LinkedHashMap<>();
    }

    public void register() throws IOException {
        this.asyncRead();
    }

    public void resetContextStatus() {
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            this.txIsolation = -1;
        }
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        this.initCharacterSet(SystemConfig.getInstance().getCharset());
        this.usrVariables.clear();
        this.sysVariables.clear();
        this.sysVariables.put("sql_mode", null);
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isRowDataFlowing() {
        return isRowDataFlowing;
    }

    public void setRowDataFlowing(boolean rowDataFlowing) {
        isRowDataFlowing = rowDataFlowing;
    }

    public TxState getXaStatus() {
        return xaStatus;
    }

    public void setXaStatus(TxState xaStatus) {
        this.xaStatus = xaStatus;
    }

    public void onConnectFailed(Throwable t) {
        if (handler instanceof MySQLConnectionHandler) {
            LOGGER.warn("unexpected failure to connect in MySQLConnectionHandler");
        } else {
            ((MySQLConnectionAuthenticator) handler).connectionError(this, t);
        }
    }

    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String newSchema) {
        String curSchema = schema;
        if (newSchema != null) {
            this.oldSchema = curSchema;
            this.schema = newSchema;
        }
    }

    public PhysicalDbInstance getDbInstance() {
        return dbInstance;
    }

    @Override
    public void setDbInstance(PhysicalDbInstance instance) {
        this.dbInstance = instance;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public HandshakeV10Packet getHandshake() {
        return handshake;
    }

    public void setHandshake(HandshakeV10Packet handshake) {
        this.handshake = handshake;
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    void setAuthenticated(boolean authenticated) {
        isAuthenticated = authenticated;
    }

    public boolean isAuthenticated() {
        return isAuthenticated;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean finishConnect() throws IOException {
        localPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
        return true;
    }

    public void setProcessor(NIOProcessor processor) {
        super.setProcessor(processor);
        processor.addBackend(this);
    }

    void authenticate() {
        AuthPacket packet = new AuthPacket();
        packet.setPacketId(1);
        packet.setMaxPacketSize(maxPacketSize);
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        packet.setCharsetIndex(charsetIndex);
        packet.setUser(user);
        try {
            String authPluginName = new String(handshake.getAuthPluginName());
            if (authPluginName.equals(new String(HandshakeV10Packet.NATIVE_PASSWORD_PLUGIN))) {
                sendAuthPacket(packet, PasswordAuthPlugin.passwd(password, handshake), authPluginName);
            } else if (authPluginName.equals(new String(HandshakeV10Packet.CACHING_SHA2_PASSWORD_PLUGIN))) {
                sendAuthPacket(packet, PasswordAuthPlugin.passwdSha256(password, handshake), authPluginName);
            } else {
                String authPluginErrorMessage = "Client don't support the password plugin " + authPluginName + ",please check the default auth Plugin";
                LOGGER.warn(authPluginErrorMessage);
                throw new RuntimeException(authPluginErrorMessage);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    private long getClientFlagSha() {
        int flag = 0;
        flag |= initClientFlags();
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        return flag;
    }

    private void sendAuthPacket(AuthPacket packet, byte[] authPassword, String authPluginName) {
        packet.setPassword(authPassword);
        packet.setClientFlags(getClientFlagSha());
        packet.setAuthPlugin(authPluginName);
        packet.setDatabase(schema);
        packet.writeWithKey(this);
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public void sendQueryCmd(String query, CharsetNames clientCharset) {
        if (isClosed) {
            closeResponseHandler("connection is closed before sending cmd");
            return;
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
        lastTime = TimeUtil.currentTimeMillis();
        int size = packet.calcPacketSize();
        if (size >= MySQLPacket.MAX_PACKET_SIZE) {
            packet.writeBigPackage(this, size);
        } else {
            packet.write(this);
        }

    }

    @Override
    public void ping() {
        write(PingPacket.PING);
    }

    private WriteToBackendTask sendQueryCmdTask(String query, CharsetNames clientCharset) {
        if (isClosed) {
            closeResponseHandler("connection is closed before sending cmd");
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
        lastTime = TimeUtil.currentTimeMillis();
        return new WriteToBackendTask(this, packet);
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

    public void executeMultiNode(RouteResultsetNode rrn, ServerConnection sc,
                                 boolean isAutoCommit) {
        String xaTxId = getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        if (!sc.isAutocommit() && !sc.isTxStart() && rrn.isModifySQL()) {
            sc.setTxStart(true);
        }
        StringBuilder synSQL = getSynSql(xaTxId, rrn, sc.getCharset(), sc.getTxIsolation(), isAutoCommit, sc.getUsrVariables(), sc.getSysVariables());
        synAndDoExecuteMultiNode(synSQL, rrn, sc.getCharset());
    }

    private StringBuilder getSynSql(String xaTxID, RouteResultsetNode rrn,
                                    CharsetNames clientCharset, int clientTxIsolation,
                                    boolean expectAutocommit, Map<String, String> usrVariables, Map<String, String> sysVariables) {
        if (rrn.getSqlType() == ServerParse.DDL) {
            isDDL = true;
        }

        int xaSyn = 0;
        if (!expectAutocommit && xaTxID != null && xaStatus == TxState.TX_INITIALIZE_STATE) {
            // clientTxIsolation = Isolation.SERIALIZABLE;TODO:NEEDED?
            xaSyn = 1;
        }

        Set<String> toResetSys = new HashSet<>();
        String setSql = getSetSQL(usrVariables, sysVariables, toResetSys);
        int setSqlFlag = setSql == null ? 0 : 1;
        int schemaSyn = StringUtil.equals(this.schema, this.oldSchema) ? 0 : 1;
        int charsetSyn = (this.charsetName.equals(clientCharset)) ? 0 : 1;
        int txIsolationSyn = (this.txIsolation == clientTxIsolation) ? 0 : 1;
        int autoCommitSyn = (this.autocommit == expectAutocommit) ? 0 : 1;
        int synCount = schemaSyn + charsetSyn + txIsolationSyn + autoCommitSyn + xaSyn + setSqlFlag;
        if (synCount == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        if (schemaSyn == 1) {
            getChangeSchemaCommand(sb, this.schema);
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
        statusSync = new StatusSync(this.schema,
                clientCharset, clientTxIsolation, expectAutocommit,
                synCount, usrVariables, sysVariables, toResetSys);
        return sb;
    }

    private void synAndDoExecuteMultiNode(StringBuilder synSQL, RouteResultsetNode rrn, CharsetNames clientCharset) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send cmd by WriteToBackendExecutor to conn[" + this + "]");
        }

        if (synSQL == null) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this.id);
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
            session.setBackendRequestTime(this.id);
        }
        taskList.add(sendQueryCmdTask(synSQL.toString(), clientCharset));
        DbleServer.getInstance().getWriteToBackendQueue().add(taskList);
        // waiting syn result...

    }

    public void execute(RouteResultsetNode rrn, ServerConnection sc,
                        boolean isAutoCommit) {
        String xaTxId = getConnXID(session.getSessionXaID(), rrn.getMultiplexNum().longValue());
        if (!sc.isAutocommit() && !sc.isTxStart() && rrn.isModifySQL()) {
            sc.setTxStart(true);
        }
        StringBuilder synSQL = getSynSql(xaTxId, rrn, sc.getCharset(), sc.getTxIsolation(), isAutoCommit, sc.getUsrVariables(), sc.getSysVariables());
        synAndDoExecute(synSQL, rrn, sc.getCharset());
    }

    public String getConnXID(String sessionXaId, long multiplexNum) {
        if (sessionXaId == null)
            return null;
        else {
            String strMultiplexNum = multiplexNum == 0 ? "" : "." + multiplexNum;
            return sessionXaId.substring(0, sessionXaId.length() - 1) + "." + this.schema + strMultiplexNum + "'";
        }
    }

    private void synAndDoExecute(StringBuilder synSQL, RouteResultsetNode rrn, CharsetNames clientCharset) {
        if (synSQL == null) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this.id);
            }
            sendQueryCmd(rrn.getStatement(), clientCharset);
            return;
        }

        // and our query sql to multi command at last
        synSQL.append(rrn.getStatement()).append(";");
        // syn and execute others
        if (session != null) {
            session.setBackendRequestTime(this.id);
        }
        this.sendQueryCmd(synSQL.toString(), clientCharset);
        // waiting syn result...

    }

    public void setRecycler(BackEndCleaner recycler) {
        this.recycler = recycler;
    }

    public void backendSpecialCleanUp() {
        isExecuting = false;
        isRowDataFlowing = false;
        this.signal();
    }

    public void signal() {
        if (isClosed()) {
            return;
        }
        this.setFlowControlled(false);
        Optional.ofNullable(recycler).ifPresent(res -> res.signal());
        recycler = null;
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
            if (value != null) {
                try {
                    new BigDecimal(value);
                } catch (NumberFormatException e) {
                    value = "`" + value + "`";
                }
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

    private static void getChangeSchemaCommand(StringBuilder sb, String schema) {
        sb.append("use `");
        sb.append(schema);
        sb.append("`;");
    }

    /**
     * by wuzh ,execute a query and ignore transaction settings for performance
     */
    public void query(String query) {
        query(query, this.autocommit);
    }

    public void query(String query, boolean isAutoCommit) {
        RouteResultsetNode rrn = new RouteResultsetNode("default", ServerParse.SELECT, query);
        StringBuilder synSQL = getSynSql(null, rrn, this.charsetName, this.txIsolation, isAutoCommit, this.getUsrVariables(), this.getSysVariables());
        synAndDoExecute(synSQL, rrn, this.charsetName);

    }

    @Override
    public long getLastTime() {
        return lastTime;
    }

    public void close() {
        close("normal", false);
    }

    public void close(String reason, boolean closeFrontConn) {
        if (closeFrontConn && session != null) {
            session.getSource().close(reason);
        } else {
            close("normal");
        }
    }

    /**
     * Only write quit packet to backend ,when the NIOSocketWR find the QuitPacket
     * closeInner() would be called
     *
     * @param reason
     */
    @Override
    public synchronized void close(final String reason) {
        if (!isClosed) {
            if (isAuthenticated && channel.isOpen()) {
                try {
                    closeReason = reason;
                    write(writeToBuffer(QuitPacket.QUIT, allocate()));
                } catch (Throwable e) {
                    LOGGER.info("error when try to quit the connection ,drop the error and close it anyway", e);
                    closeInner(reason);
                }
            } else {
                closeInner(reason);
            }
            this.setExecuting(false);
            this.setRowDataFlowing(false);
            this.signal();
        } else {
            this.cleanup();
            if (this.respHandler != null) {
                closeResponseHandler(reason == null ? closeReason : reason);
            }
        }
    }

    @Override
    public void startFlowControl(BackendConnection bcon) {
        LOGGER.info("Session start flow control " + this);
        this.setFlowControlled(true);
    }

    @Override
    public void stopFlowControl() {
        LOGGER.info("Session stop flow control " + this);
        this.setFlowControlled(false);
    }

    public long getOldTimestamp() {
        return oldTimestamp;
    }

    @Override
    public void setOldTimestamp(long oldTimestamp) {
        this.oldTimestamp = oldTimestamp;
    }

    boolean isComplexQuery() {
        return complexQuery;
    }

    public void setComplexQuery(boolean complexQuery) {
        this.complexQuery = complexQuery;
    }


    private void closeResponseHandler(final String reason) {
        final ResponseHandler handler = respHandler;
        final MySQLConnection conn = this;
        DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
            try {
                conn.setExecuting(false);
                conn.setRowDataFlowing(false);
                conn.signal();
                handler.connectionClose(conn, reason);
                respHandler = null;
            } catch (Throwable e) {
                LOGGER.warn("get error close mysql connection ", e);
            }
        });
    }

    /**
     * MySQLConnection inner resource clear
     * Only used in Net Error OR final resource clear
     *
     * @param reason
     */
    public void closeInner(final String reason) {
        innerTerminate(reason == null ? closeReason : reason);
        if (this.respHandler != null) {
            closeResponseHandler(reason == null ? closeReason : reason);
        }
    }

    @Override
    public void connect() {
        if (channel instanceof AsynchronousSocketChannel) {
            ((AsynchronousSocketChannel) channel).connect(
                    new InetSocketAddress(getHost(), getPort()), this,
                    (CompletionHandler) DbleServer.getInstance().getConnector());
        } else {
            ((NIOConnector) DbleServer.getInstance().getConnector()).postConnect(this);
        }
    }

    /**
     * close connection without closeResponseHandler
     */
    @Override
    public synchronized void closeWithoutRsp(String reason) {
        this.respHandler = null;
        this.close(reason);
    }

    private synchronized void innerTerminate(String reason) {
        if (!isClosed()) {
            super.close(reason);
            // when it happens during the connection authentication phase
            if (!isAuthenticated) {
                onConnectFailed(new Exception(reason));
            } else {
                // remove the it from the ConnectionPool
                if (dbInstance != null) {
                    dbInstance.close(this);
                }
                // else: it does not belong to the ConnectionPool, eg: heartbeat connection
            }

        }
    }

    public void commit() {
        COMMIT.write(this);
    }

    public void execCmd(String cmd) {
        this.sendQueryCmd(cmd, this.charsetName);
    }

    public void rollback() {
        ROLLBACK.write(this);
    }

    public void release() {
        if (!metaDataSynced) { // indicate connection not normal finished
            // ,and
            // we can't know it's syn status ,so
            // close
            // it
            LOGGER.info("can't sure connection syn result,so close it " + this);
            this.respHandler = null;
            this.close("syn status unknown ");
            return;
        }
        if (this.usrVariables.size() > 0) {
            this.respHandler = null;
            this.close("close for clear usrVariables");
            return;
        }
        if (this.isRowDataFlowing()) {
            if (logResponse.compareAndSet(false, true)) {
                session.setBackendResponseEndTime(this);
            }
            if (SystemConfig.getInstance().getEnableAsyncRelease() == 1) {
                DbleServer.getInstance().getComplexQueryExecutor().execute(new BackEndRecycleRunnable(this));
            } else {
                new BackEndRecycleRunnable(this).run();
            }
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
        dbInstance.release(this);
    }


    public boolean isExecuting() {
        return isExecuting;
    }

    @Override
    public void disableRead() {
        this.getSocketWR().disableRead();
    }

    @Override
    public void enableRead() {
        if (!isClosed()) {
            this.getSocketWR().enableRead();
        }
    }

    public void setExecuting(boolean executing) {
        isExecuting = executing;
    }

    public boolean setResponseHandler(ResponseHandler queryHandler) {
        if (handler instanceof MySQLConnectionHandler) {
            ((MySQLConnectionHandler) handler).setResponseHandler(queryHandler);
            respHandler = queryHandler;
            return true;
        } else if (queryHandler != null) {
            LOGGER.info("set not MySQLConnectionHandler " + queryHandler.getClass().getCanonicalName());
        }
        return false;
    }

    public ResponseHandler getRespHandler() {
        return respHandler;
    }

    public void setSession(NonBlockingSession session) {
        this.session = session;
        if (handler instanceof MySQLConnectionHandler) {
            ((MySQLConnectionHandler) handler).setSession(session);
        }
    }

    public boolean isTesting() {
        return testing;
    }

    public void setTesting(boolean testing) {
        this.testing = testing;
    }

    @Override
    public boolean isFromSlaveDB() {
        return fromSlaveDB;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("MySQLConnection [backendId=");
        result.append(id);
        result.append(", lastTime=");
        result.append(lastTime);
        result.append(", user=");
        result.append(user);
        result.append(", schema=");
        result.append(schema);
        result.append(", old schema=");
        result.append(oldSchema);
        result.append(", fromSlaveDB=");
        result.append(fromSlaveDB);
        result.append(", mysqlId=");
        result.append(threadId);
        result.append(",");
        result.append(charsetName.toString());
        result.append(", txIsolation=");
        result.append(txIsolation);
        result.append(", autocommit=");
        result.append(autocommit);
        result.append(", attachment=");
        result.append(attachment);
        result.append(", respHandler=");
        result.append(respHandler);
        result.append(", host=");
        result.append(host);
        result.append(", port=");
        result.append(port);
        result.append(", statusSync=");
        result.append(statusSync);
        result.append(", writeQueue=");
        result.append(this.getWriteQueue().size());
        result.append(", xaStatus=");
        result.append(xaStatus);
        if (sysVariables.size() > 0) {
            result.append(", ");
            result.append(getStringOfSysVariables());
        }
        if (usrVariables.size() > 0) {
            result.append(", ");
            result.append(getStringOfUsrVariables());
        }
        result.append("]");
        return result.toString();
    }

    @Override
    public void connectionCount() {
        return;
    }

    public String compactInfo() {
        return "MySQLConnection host=" + host + ", port=" + port + ", schema=" + schema + ", mysqlid=" + threadId;
    }

    @Override
    public boolean isDDL() {
        return isDDL;
    }

    @Override
    public int getTxIsolation() {
        return txIsolation;
    }

    /**
     * @return if synchronization finished and execute-sql has already been sent
     * before
     */
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

    public AtomicBoolean getLogResponse() {
        return logResponse;
    }

    public long getConnectionTimeout() {
        return this.dbInstance != null ? this.dbInstance.getConfig().getPoolConfig().getConnectionTimeout() : connectionTimeout;
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
            super();
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

        boolean synAndExecuted(MySQLConnection conn) {
            int remains = synCmdCount.decrementAndGet();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("synAndExecuted " + remains + ",conn info:" + conn);
            }
            if (remains == 0) { // syn command finished
                this.updateConnectionInfo(conn);
                conn.metaDataSynced = true;
                return false;
            }
            return remains < 0;
        }

        private void updateConnectionInfo(MySQLConnection conn) {
            if (schema != null) {
                conn.schema = schema;
                conn.oldSchema = conn.schema;
            }
            if (clientCharset != null) {
                conn.setCharsetName(clientCharset);
            }
            if (txtIsolation != null) {
                conn.txIsolation = txtIsolation;
            }
            if (autocommit != null) {
                conn.autocommit = autocommit;
            }
            conn.sysVariables = sysVariables;
            conn.usrVariables = usrVariables;
        }
    }
}
