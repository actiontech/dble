/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.SecurityUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.btrace.provider.XaDelayProvider;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.net.BackendAIOConnection;
import com.actiontech.dble.net.handler.BackEndCleaner;
import com.actiontech.dble.net.handler.BackEndRecycleRunnable;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.actiontech.dble.util.exception.UnknownTxIsolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.channels.NetworkChannel;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mycat
 */
public class MySQLConnection extends BackendAIOConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnection.class);
    private static final long CLIENT_FLAGS = initClientFlags();
    private volatile long lastTime;
    private volatile String schema = null;
    private volatile String oldSchema;
    private volatile boolean borrowed = false;
    private volatile boolean modifiedSQLExecuted = false;
    private volatile boolean isDDL = false;
    private volatile boolean isRunning = false;
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

    private volatile BackEndCleaner recycler = null;

    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = DbleServer.getInstance().getConfig().getSystem().getUseCompression() == 1;
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

    private MySQLDataSource pool;
    private boolean fromSlaveDB;
    private long threadId;
    private HandshakeV10Packet handshake;
    private long clientFlags;
    private boolean isAuthenticated;
    private String user;
    private String password;
    private Object attachment;
    private ResponseHandler respHandler;

    private final AtomicBoolean isQuit;

    public MySQLConnection(NetworkChannel channel, boolean fromSlaveDB, boolean isNoSchema) {
        super(channel);
        this.clientFlags = CLIENT_FLAGS;
        if (isNoSchema) {
            this.clientFlags = (CLIENT_FLAGS >> 4) << 4 | (CLIENT_FLAGS & 7);
        } else {
            this.clientFlags = CLIENT_FLAGS;
        }
        this.lastTime = TimeUtil.currentTimeMillis();
        this.isQuit = new AtomicBoolean(false);
        this.autocommit = true;
        this.fromSlaveDB = fromSlaveDB;
        /* if the txIsolation in server.xml is different from the isolation level in MySQL node,
        *  it need to sync the status firstly for new idle connection*/
        this.txIsolation = -1;
        this.complexQuery = false;
        this.usrVariables = new LinkedHashMap<>();
        this.sysVariables = new LinkedHashMap<>();
    }

    public void resetContextStatus() {
        this.txIsolation = -1;
        this.autocommit = true;
        this.initCharacterSet(DbleServer.getInstance().getConfig().getSystem().getCharset());
        this.usrVariables.clear();
        this.sysVariables.clear();
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public TxState getXaStatus() {
        return xaStatus;
    }

    public void setXaStatus(TxState xaStatus) {
        this.xaStatus = xaStatus;
    }

    public void onConnectFailed(Throwable t) {
        if (handler instanceof MySQLConnectionHandler) {
            MySQLConnectionHandler theHandler = (MySQLConnectionHandler) handler;
            theHandler.connectionError(t);
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

    public MySQLDataSource getPool() {
        return pool;
    }

    public void setPool(MySQLDataSource pool) {
        this.pool = pool;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
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

    public String getPassword() {
        return password;
    }

    void authenticate() {
        AuthPacket packet = new AuthPacket();
        packet.setPacketId(1);
        packet.setClientFlags(clientFlags);
        packet.setMaxPacketSize(maxPacketSize);
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(DbleServer.getInstance().getConfig().getSystem().getCharset());
        packet.setCharsetIndex(charsetIndex);

        packet.setUser(user);
        try {
            packet.setPassword(passwd(password, handshake));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
        packet.setDatabase(schema);
        packet.write(this);
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

    public boolean isClosedOrQuit() {
        return isClosed() || isQuit.get();
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
        lastTime = TimeUtil.currentTimeMillis();
        packet.write(this);
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
        if (!modifiedSQLExecuted && rrn.isModifySQL()) {
            modifiedSQLExecuted = true;
        }
        if (rrn.getSqlType() == ServerParse.DDL) {
            isDDL = true;
        }
        String xaTxId = getConnXID(session);
        if (!sc.isAutocommit() && !sc.isTxStart() && modifiedSQLExecuted) {
            sc.setTxStart(true);
        }
        synAndDoExecuteMultiNode(xaTxId, rrn, sc.getCharset(), sc.getTxIsolation(), isAutoCommit, sc.getUsrVariables(), sc.getSysVariables());
    }

    private void synAndDoExecuteMultiNode(String xaTxID, RouteResultsetNode rrn,
                                          CharsetNames clientCharset, int clientTxIsolation,
                                          boolean expectAutocommit, Map<String, String> usrVariables, Map<String, String> sysVariables) {
        String xaCmd = null;
        boolean conAutoCommit = this.autocommit;
        String conSchema = this.schema;
        int xaSyn = 0;
        if (!expectAutocommit && xaTxID != null && xaStatus == TxState.TX_INITIALIZE_STATE) {
            // clientTxIsolation = Isolation.SERIALIZABLE;TODO:NEEDED?
            xaCmd = "XA START " + xaTxID + ';';
            this.xaStatus = TxState.TX_STARTED_STATE;
            xaSyn = 1;
        }
        Set<String> toResetSys = new HashSet<>();
        String setSql = getSetSQL(usrVariables, sysVariables, toResetSys);
        int setSqlFlag = setSql == null ? 0 : 1;
        int schemaSyn = conSchema.equals(oldSchema) ? 0 : 1;
        int charsetSyn = (charsetName.equals(clientCharset)) ? 0 : 1;
        int txIsolationSyn = (txIsolation == clientTxIsolation) ? 0 : 1;
        int autoCommitSyn = (conAutoCommit == expectAutocommit) ? 0 : 1;
        int synCount = schemaSyn + charsetSyn + txIsolationSyn + autoCommitSyn + xaSyn + setSqlFlag;
        if (synCount == 0) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this.id);
            }
            DbleServer.getInstance().getWriteToBackendQueue().add(Collections.singletonList(sendQueryCmdTask(rrn.getStatement(), clientCharset)));
            return;
        }
        CommandPacket schemaCmd = null;
        StringBuilder sb = new StringBuilder();
        if (schemaSyn == 1) {
            schemaCmd = getChangeSchemaCommand(conSchema);
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
        if (xaCmd != null) {
            XaDelayProvider.delayBeforeXaStart(rrn.getName(), xaTxID);
            sb.append(xaCmd);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con need syn ,total syn cmd " + synCount +
                    " commands " + sb.toString() + "schema change:" +
                    (schemaCmd != null) + " con:" + this);
        }
        metaDataSynced = false;
        statusSync = new StatusSync(conSchema,
                clientCharset, clientTxIsolation, expectAutocommit,
                synCount, usrVariables, sysVariables, toResetSys);
        // syn schema
        List<WriteToBackendTask> taskList = new ArrayList<>(2);
        if (schemaCmd != null) {
            taskList.add(new WriteToBackendTask(this, schemaCmd));
        }
        // and our query sql to multi command at last
        sb.append(rrn.getStatement()).append(";");
        // syn and execute others
        if (session != null) {
            session.setBackendRequestTime(this.id);
        }
        taskList.add(sendQueryCmdTask(sb.toString(), clientCharset));
        DbleServer.getInstance().getWriteToBackendQueue().add(taskList);
        // waiting syn result...

    }

    public void execute(RouteResultsetNode rrn, ServerConnection sc,
                        boolean isAutoCommit) {
        if (!modifiedSQLExecuted && rrn.isModifySQL()) {
            modifiedSQLExecuted = true;
        }
        if (rrn.getSqlType() == ServerParse.DDL) {
            isDDL = true;
        }
        String xaTxId = getConnXID(session);
        if (!sc.isAutocommit() && !sc.isTxStart() && modifiedSQLExecuted) {
            sc.setTxStart(true);
        }
        synAndDoExecute(xaTxId, rrn, sc.getCharset(), sc.getTxIsolation(), isAutoCommit, sc.getUsrVariables(), sc.getSysVariables());
    }

    public String getConnXID(NonBlockingSession nonBlockingSession) {
        if (nonBlockingSession.getSessionXaID() == null)
            return null;
        else {
            String sessionXaID = nonBlockingSession.getSessionXaID();
            return sessionXaID.substring(0, sessionXaID.length() - 1) + "." + this.schema + "'";
        }
    }

    private void synAndDoExecute(String xaTxID, RouteResultsetNode rrn,
                                 CharsetNames clientCharset, int clientTxIsolation,
                                 boolean expectAutocommit, Map<String, String> usrVariables, Map<String, String> sysVariables) {
        String xaCmd = null;
        boolean conAutoCommit = this.autocommit;
        String conSchema = this.schema;
        int xaSyn = 0;
        if (!expectAutocommit && xaTxID != null && xaStatus == TxState.TX_INITIALIZE_STATE) {
            // clientTxIsolation = Isolation.SERIALIZABLE;TODO:NEEDED?
            xaCmd = "XA START " + xaTxID + ';';
            this.xaStatus = TxState.TX_STARTED_STATE;
            xaSyn = 1;
        }
        Set<String> toResetSys = new HashSet<>();
        String setSql = getSetSQL(usrVariables, sysVariables, toResetSys);
        int setSqlFlag = setSql == null ? 0 : 1;
        int schemaSyn = StringUtil.equals(conSchema, oldSchema) ? 0 : 1;
        int charsetSyn = (charsetName.equals(clientCharset)) ? 0 : 1;
        int txIsolationSyn = (txIsolation == clientTxIsolation) ? 0 : 1;
        int autoCommitSyn = (conAutoCommit == expectAutocommit) ? 0 : 1;
        int synCount = schemaSyn + charsetSyn + txIsolationSyn + autoCommitSyn + xaSyn + setSqlFlag;
        if (synCount == 0) {
            // not need syn connection
            if (session != null) {
                session.setBackendRequestTime(this.id);
            }
            sendQueryCmd(rrn.getStatement(), clientCharset);
            return;
        }
        CommandPacket schemaCmd = null;
        StringBuilder sb = new StringBuilder();
        if (schemaSyn == 1) {
            schemaCmd = getChangeSchemaCommand(conSchema);
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
        if (xaCmd != null) {
            XaDelayProvider.delayBeforeXaStart(rrn.getName(), xaTxID);
            sb.append(xaCmd);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con need syn ,total syn cmd " + synCount +
                    " commands " + sb.toString() + "schema change:" +
                    (schemaCmd != null) + " con:" + this);
        }
        metaDataSynced = false;
        statusSync = new StatusSync(conSchema,
                clientCharset, clientTxIsolation, expectAutocommit,
                synCount, usrVariables, sysVariables, toResetSys);
        // syn schema
        if (schemaCmd != null) {
            schemaCmd.write(this);
        }
        // and our query sql to multi command at last
        sb.append(rrn.getStatement()).append(";");
        // syn and execute others
        if (session != null) {
            session.setBackendRequestTime(this.id);
        }
        this.sendQueryCmd(sb.toString(), clientCharset);
        // waiting syn result...

    }

    public void setRecycler(BackEndCleaner recycler) {
        this.recycler = recycler;
    }

    public void signal() {
        if (recycler != null) {
            recycler.signal();
        }
    }

    private String getSetSQL(Map<String, String> usrVars, Map<String, String> sysVars, Set<String> toResetSys) {
        //new final var
        List<Pair<String, String>> setVars = new ArrayList<>();
        Map<String, String> tmpSysVars = new HashMap<>();
        //tmp add all backend sysVariables
        tmpSysVars.putAll(sysVariables);
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

    private static CommandPacket getChangeSchemaCommand(String schema) {
        CommandPacket cmd = new CommandPacket();
        cmd.setPacketId(0);
        cmd.setCommand(MySQLPacket.COM_INIT_DB);
        cmd.setArg(schema.getBytes());
        return cmd;
    }

    /**
     * by wuzh ,execute a query and ignore transaction settings for performance
     */
    public void query(String query) {
        RouteResultsetNode rrn = new RouteResultsetNode("default",
                ServerParse.SELECT, query);
        synAndDoExecute(null, rrn, this.charsetName, this.txIsolation, true, this.getUsrVariables(), this.getSysVariables());

    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public void quit() {
        if (isQuit.compareAndSet(false, true) && !isClosed()) {
            if (isAuthenticated) {
                write(writeToBuffer(QuitPacket.QUIT, allocate()));
                innerTerminate("normal");
            } else {
                close("normal");
            }
        }
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

    @Override
    public void close(final String reason) {
        if (!isClosed.get()) {
            if (isAuthenticated) {
                isQuit.set(true);
                if (channel.isOpen()) {
                    try {
                        write(writeToBuffer(QuitPacket.QUIT, allocate()));
                    } catch (Throwable e) {
                        LOGGER.info("error when try to quite the connection ,drop the error and close it anyway");
                    }
                }
            }
            this.setRunning(false);
            this.signal();
            innerTerminate(reason);
        }
        if (this.respHandler != null) {
            closeResponseHandler(reason);
        }
    }

    private void closeResponseHandler(final String reason) {
        final ResponseHandler handler = respHandler;
        final MySQLConnection conn = this;
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    conn.setRunning(false);
                    conn.signal();
                    handler.connectionClose(conn, reason);
                    respHandler = null;
                } catch (Throwable e) {
                    LOGGER.warn("get error close mysqlconnection ", e);
                }
            }
        });
    }

    public void closeInner(final String reason) {
        if (!isClosed.get()) {
            innerTerminate(reason);
        }
        if (this.respHandler != null) {
            closeResponseHandler(reason);
        }
    }

    @Override
    public void terminate(String reason) {
        if (!isClosed.get()) {
            innerTerminate(reason);
        }
    }

    private synchronized void innerTerminate(String reason) {
        if (!isClosed()) {
            isQuit.set(true);
            super.close(reason);
            pool.connectionClosed(this);
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
        if (this.isRunning()) {
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
        modifiedSQLExecuted = false;
        isDDL = false;
        testing = false;
        setResponseHandler(null);
        setSession(null);
        logResponse.set(false);
        pool.releaseChannel(this);
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

    public void writeQueueAvailable() {
        if (respHandler != null) {
            respHandler.writeQueueAvailable();
        }
    }

    private static byte[] passwd(String pass, HandshakeV10Packet hs)
            throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] passwd = pass.getBytes();
        int sl1 = hs.getSeed().length;
        int sl2 = hs.getRestOfScrambleBuff().length;
        byte[] seed = new byte[sl1 + sl2];
        System.arraycopy(hs.getSeed(), 0, seed, 0, sl1);
        System.arraycopy(hs.getRestOfScrambleBuff(), 0, seed, sl1, sl2);
        return SecurityUtil.scramble411(passwd, seed);
    }

    @Override
    public boolean isFromSlaveDB() {
        return fromSlaveDB;
    }

    @Override
    public boolean isBorrowed() {
        return borrowed;
    }

    @Override
    public void setBorrowed(boolean borrowed) {
        this.lastTime = TimeUtil.currentTimeMillis();
        this.borrowed = borrowed;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("MySQLConnection [id=");
        result.append(id);
        result.append(", lastTime=");
        result.append(lastTime);
        result.append(", user=");
        result.append(user);
        result.append(", schema=");
        result.append(schema);
        result.append(", old schema=");
        result.append(oldSchema);
        result.append(", borrowed=");
        result.append(borrowed);
        result.append(", fromSlaveDB=");
        result.append(fromSlaveDB);
        result.append(", threadId=");
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
        result.append(", modifiedSQLExecuted=");
        result.append(modifiedSQLExecuted);
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

    public String compactInfo() {
        return "MySQLConnection host=" + host + ", port=" + port + ", schema=" + schema;
    }

    @Override
    public boolean isModifiedSQLExecuted() {
        return modifiedSQLExecuted;
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
            return true;
        } else {
            boolean executed = sync.synAndExecuted(this);
            if (executed) {
                statusSync = null;
            }
            return executed;
        }

    }

    public AtomicBoolean getLogResponse() {
        return logResponse;
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
            } else if (remains < 0) {
                return true;
            }
            return false;
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
