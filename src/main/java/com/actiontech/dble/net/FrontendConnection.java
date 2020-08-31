/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.util.AuthUtil;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.handler.*;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.SerializableLock;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.RandomUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mycat
 */
public abstract class FrontendConnection extends AbstractConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendConnection.class);

    protected byte[] seed;
    protected String user;
    protected String schema;
    private long clientFlags;

    protected String executeSql;

    protected FrontendPrivileges privileges;
    protected FrontendQueryHandler queryHandler;
    protected FrontendPrepareHandler prepareHandler;
    protected LoadDataInfileHandler loadDataInfileHandler;

    protected boolean isAccepted;
    protected boolean isAuthenticated;
    private boolean userReadOnly = true;
    private boolean sessionReadOnly = false;
    private volatile boolean multStatementAllow = false;

    public FrontendConnection(NetworkChannel channel) throws IOException {
        super(channel);
        InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        InetSocketAddress remoteAddress = null;
        if (channel instanceof SocketChannel) {
            remoteAddress = (InetSocketAddress) ((SocketChannel) channel).getRemoteAddress();
        } else if (channel instanceof AsynchronousSocketChannel) {
            remoteAddress = (InetSocketAddress) ((AsynchronousSocketChannel) channel).getRemoteAddress();
        } else {
            throw new RuntimeException("FrontendConnection type is" + channel.getClass());
        }

        this.host = remoteAddress.getHostString();
        this.port = localAddress.getPort();
        this.localPort = remoteAddress.getPort();
        this.handler = new FrontendAuthenticator(this);
    }

    public FrontendConnection() {
        /* just for unit test */
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public void setAccepted(boolean accepted) {
        isAccepted = accepted;
    }

    public void setClientFlags(long clientFlags) {
        this.clientFlags = clientFlags;
    }

    public long getClientFlags() {
        return clientFlags;
    }

    public void setProcessor(NIOProcessor processor) {
        super.setProcessor(processor);
        processor.addFrontend(this);
    }

    public LoadDataInfileHandler getLoadDataInfileHandler() {
        return loadDataInfileHandler;
    }

    public void setLoadDataInfileHandler(LoadDataInfileHandler loadDataInfileHandler) {
        this.loadDataInfileHandler = loadDataInfileHandler;
    }

    public boolean isMultStatementAllow() {
        return multStatementAllow;
    }

    public void setMultStatementAllow(boolean multStatementAllow) {
        this.multStatementAllow = multStatementAllow;
    }

    public void setQueryHandler(FrontendQueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public void setPrepareHandler(FrontendPrepareHandler prepareHandler) {
        this.prepareHandler = prepareHandler;
    }

    public void setAuthenticated(boolean authenticated) {
        this.isAuthenticated = authenticated;
    }

    public FrontendPrivileges getPrivileges() {
        return privileges;
    }

    public void setPrivileges(FrontendPrivileges privileges) {
        this.privileges = privileges;
    }

    public void setSessionReadOnly(boolean sessionReadOnly) {
        this.sessionReadOnly = sessionReadOnly;
    }

    public boolean isReadOnly() {
        return userReadOnly || sessionReadOnly;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
        this.userReadOnly = privileges.isReadOnly(user);
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        if (schema != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            schema = schema.toLowerCase();
        }
        this.schema = schema;
    }

    public String getExecuteSql() {
        return executeSql;
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
    }

    public byte[] getSeed() {
        return seed;
    }

    public void initCharsetIndex(int ci) {
        String name = CharsetUtil.getCharset(ci);
        if (name != null) {
            charsetName.setClient(name);
            charsetName.setResults(name);
            charsetName.setCollation(CharsetUtil.getDefaultCollation(name));
        }
    }

    public void writeErrMessage(String sqlState, String msg, int vendorCode) {
        writeErrMessage((byte) 1, vendorCode, sqlState, msg);
    }

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage((byte) 1, vendorCode, msg);
    }

    public void writeErrMessage(byte id, int vendorCode, String msg) {
        writeErrMessage(id, vendorCode, "HY000", msg);
    }

    protected void writeErrMessage(byte id, int vendorCode, String sqlState, String msg) {
        markFinished();
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(id);
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, charsetName.getResults()));
        err.setMessage(StringUtil.encode(msg, charsetName.getResults()));
        err.write(this);
    }

    protected abstract void setRequestTime();

    public abstract void preparePushToQueue();

    public abstract void finishPushToQueue();

    public abstract void startProcess();

    protected abstract void markFinished();

    public void initDB(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String db = null;
        try {
            db = mm.readString(charsetName.getClient());
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charsetName.getClient() + "'");
            return;
        }
        if (db != null && DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            db = db.toLowerCase();
        }
        // check schema
        if (db == null || !privileges.schemaExists(db)) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }

        if (!privileges.userExists(user, host)) {
            writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "'");
            return;
        }

        Set<String> schemas = privileges.getUserSchemas(user);
        if (schemas == null || schemas.size() == 0 || schemas.contains(db)) {
            this.schema = db;
            write(writeToBuffer(OkPacket.OK, allocate()));
        } else {
            String s = "Access denied for user '" + user + "' to database '" + db + "'";
            writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
        }
    }


    public void loadDataInfileStart(String sql) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.start(sql);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }

        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile sql is not  unsupported!");
        }
    }

    public void loadDataInfileData(byte[] data) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.handle(data);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile  data is not  unsupported!");
        }

    }

    public void loadDataInfileEnd(byte packID) {
        if (loadDataInfileHandler != null) {
            try {
                loadDataInfileHandler.end(packID);
            } catch (Exception e) {
                LOGGER.info("load data error", e);
                writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
            }
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile end is not  unsupported!");
        }
    }


    public void query(String sql) {
        if (sql == null || sql.length() == 0) {
            writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return;
        }
        sql = sql.trim();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.valueOf(this) + " " + sql);
        }
        // remove last ';'
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        // record SQL
        this.setExecuteSql(sql);

        if (!privileges.checkFirewallSQLPolicy(user, sql)) {
            writeErrMessage(ErrorCode.ERR_WRONG_USED, "The statement is unsafe SQL, reject for user '" + user + "'");
            return;
        }

        // execute
        if (queryHandler != null) {
            queryHandler.setReadOnly(userReadOnly);
            queryHandler.setSessionReadOnly(sessionReadOnly);
            queryHandler.query(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Query unsupported!");
        }
    }

    public void query(byte[] data) {
        String sql = null;
        try {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            sql = mm.readString(charsetName.getClient());
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charsetName.getClient() + "'");
            return;
        }
        if (this instanceof ServerConnection) {
            SerializableLock.getInstance().lock(this.id);
        }
        this.query(sql);
    }

    public void stmtPrepare(byte[] data) {
        if (prepareHandler != null) {
            MySQLMessage mm = new MySQLMessage(data);
            mm.position(5);
            String sql = null;
            try {
                sql = mm.readString(charsetName.getClient());
            } catch (UnsupportedEncodingException e) {
                writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
                        "Unknown charset '" + charsetName.getClient() + "'");
                return;
            }
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }

            // record SQL
            this.setExecuteSql(sql);

            // execute
            prepareHandler.prepare(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void stmtSendLongData(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.sendLongData(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void setOption(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data); //see sql\protocol_classic.cc parse_packet
        if (mm.length() == 7) {
            mm.position(5);
            int optCommand = mm.readUB2();
            if (optCommand == 0) {
                this.multStatementAllow = true;
                write(writeToBuffer(EOFPacket.EOF, allocate()));
                return;
            } else if (optCommand == 1) {
                this.multStatementAllow = false;
                write(writeToBuffer(EOFPacket.EOF, allocate()));
                return;
            }
        }
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Set Option ERROR!");
    }

    //  mysql-server\sql\sql_class.cc void THD::cleanup_connection(void)
    public void resetConnection() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("resetConnection request");
        }
        innerResetConnection();
        this.write(OkPacket.OK);
    }

    private void innerResetConnection() {
        if (this instanceof ServerConnection) {
            ServerConnection sc = (ServerConnection) this;
            sc.innerCleanUp();
        }
    }

    public void changeUser(byte[] data, ChangeUserPacket changeUserPacket, AtomicBoolean isAuthSwitch) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("changeUser request");
        }
        innerResetConnection();
        changeUserPacket.read(data);
        if ("mysql_native_password".equals(changeUserPacket.getAuthPlugin())) {
            AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(changeUserPacket.getAuthPlugin().getBytes(), this.getSeed());
            authSwitch.setPacketId(changeUserPacket.getPacketId() + 1);
            isAuthSwitch.set(true);
            authSwitch.write(this);
        } else {
            writeErrMessage((byte) (changeUserPacket.getPacketId() + 1), ErrorCode.ER_PLUGIN_IS_NOT_LOADED, "NOT SUPPORT THIS PLUGIN!");
        }
    }

    public void changeUserAuthSwitch(byte[] data, ChangeUserPacket changeUserPacket) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("changeUser AuthSwitch request");
        }
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        changeUserPacket.setPassword(authSwitchResponse.getAuthPluginData());
        String errMsg = AuthUtil.authority(this, changeUserPacket.getUser(), changeUserPacket.getPassword(), changeUserPacket.getDatabase(), false);
        byte packetId = (byte) (authSwitchResponse.getPacketId() + 1);
        if (errMsg == null) {
            changeUserSuccess(changeUserPacket, packetId);
        } else {
            writeErrMessage(packetId, ErrorCode.ER_ACCESS_DENIED_ERROR, errMsg);
        }
    }

    private void changeUserSuccess(ChangeUserPacket newUser, byte packetId) {
        this.setUser(newUser.getUser());
        this.setSchema(newUser.getDatabase());
        this.initCharsetIndex(newUser.getCharsetIndex());
        OkPacket ok = new OkPacket();
        ok.read(OkPacket.OK);
        ok.setPacketId(packetId);
        ok.write(this);
    }

    public void stmtReset(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.reset(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void stmtExecute(byte[] data, Queue<byte[]> dataqueue) {
        byte[] sendData = dataqueue.poll();
        while (sendData != null) {
            this.stmtSendLongData(sendData);
            sendData = dataqueue.poll();
        }
        if (prepareHandler != null) {
            prepareHandler.execute(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void stmtClose(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.close(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void ping() {
        write(writeToBuffer(OkPacket.OK, allocate()));
    }

    public void heartbeat(byte[] data) {
        write(writeToBuffer(OkPacket.OK, allocate()));
    }

    public void kill(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    @Override
    public void register() throws IOException {
        if (!isClosed) {

            // generate auth data
            byte[] rand1 = RandomUtil.randomBytes(8);
            byte[] rand2 = RandomUtil.randomBytes(12);

            // save  auth data
            byte[] rand = new byte[rand1.length + rand2.length];
            System.arraycopy(rand1, 0, rand, 0, rand1.length);
            System.arraycopy(rand2, 0, rand, rand1.length, rand2.length);
            this.seed = rand;

            HandshakeV10Packet hs = new HandshakeV10Packet();
            hs.setPacketId(0);
            hs.setProtocolVersion(Versions.PROTOCOL_VERSION);  // [0a] protocol version   V10
            hs.setServerVersion(Versions.getServerVersion());
            hs.setThreadId(id);
            hs.setSeed(rand1);
            hs.setServerCapabilities(getServerCapabilities());
            int charsetIndex = CharsetUtil.getCharsetDefaultIndex(DbleServer.getInstance().getConfig().getSystem().getCharset());
            hs.setServerCharsetIndex((byte) (charsetIndex & 0xff));
            hs.setServerStatus(2);
            hs.setRestOfScrambleBuff(rand2);
            hs.write(this);
        }
    }

    @Override
    public void handle(final byte[] data) {
        setRequestTime();
        if (isSupportCompress()) {
            List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, decompressUnfinishedDataQueue);
            for (byte[] pack : packs) {
                if (pack.length != 0) {
                    rawHandle(pack);
                }
            }

        } else {
            rawHandle(data);
        }
    }

    public void rawHandle(final byte[] data) {

        //load data infile  client send empty packet which size is 4
        if (data.length == 4 && data[0] == 0 && data[1] == 0 && data[2] == 0) {
            // load in data empty packet
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    loadDataInfileEnd(data[3]);
                }
            });
            return;
        }
        //when TERMINATED char of load data infile is \001
        if (data.length > 4 && data[0] == 1 && data[1] == 0 && data[2] == 0 && data[3] == 0 && data[4] == MySQLPacket.COM_QUIT) {
            this.getProcessor().getCommands().doQuit();
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    close("quit cmd");
                }
            });
            return;
        }
        handler.handle(data);

    }

    protected int getServerCapabilities() {
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
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        return flag;
    }

    @Override
    public String toString() {
        return "[thread=" +
                Thread.currentThread().getName() + ",class=" +
                getClass().getSimpleName() + ",frontId=" + id +
                ",host=" + host + ",port=" + port +
                ",schema=" + schema + ']';
    }

    @Override
    public synchronized void close(String reason) {
        super.close(isAuthenticated ? reason : "");
    }

    public abstract void killAndClose(String reason);

    @Override
    public void connectionCount() {
        if (this.isAuthenticated) {
            FrontendUserManager.getInstance().countDown(user, (this instanceof ManagerConnection));
        }
    }
}
