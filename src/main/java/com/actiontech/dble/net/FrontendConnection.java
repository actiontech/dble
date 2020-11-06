/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.handler.FrontendQueryHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.HandshakeV10Packet;
import com.actiontech.dble.server.response.Ping;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.util.RandomUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * @author mycat
 */
public abstract class FrontendConnection extends AbstractConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendConnection.class);
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private byte[] seed;
    protected UserName user;
    private long clientFlags;
    protected String schema;

    private boolean isAuthenticated;

    protected FrontendQueryHandler queryHandler;
    protected String executeSql;
    private final long idleTimeout = SystemConfig.getInstance().getIdleTimeout();

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


    public void setAuthenticated(boolean authenticated) {
        this.isAuthenticated = authenticated;
    }

    public UserName getUser() {
        return user;
    }

    public void setQueryHandler(FrontendQueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public String getExecuteSql() {
        return executeSql;
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
    }

    public void setUser(UserName user) {
        this.user = user;
    }

    public byte[] getSeed() {
        return seed;
    }

    public String getSchema() {
        return schema;
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

    public boolean isAuthenticated() {
        return isAuthenticated;
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
        query(sql);
    }

    public void query(String sql) {
        if (sql == null || sql.length() == 0) {
            writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return;
        }
        sql = sql.trim();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(this + " " + sql);
        }
        // remove last ';'
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        // record SQL
        this.setExecuteSql(sql);

        handlerQuery(sql);

    }


    public void ping() {
        Ping.response(this);
    }

    protected abstract void handlerQuery(String sql);

    protected abstract void setRequestTime();

    public abstract void startProcess();

    protected abstract void markFinished();

    protected abstract void setSchema(String schema);

    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime.get(), lastReadTime.get()) + idleTimeout;
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime.get(), lastReadTime.get()) + AUTH_TIMEOUT;
        }
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
            int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
            hs.setServerCharsetIndex((byte) (charsetIndex & 0xff));
            hs.setServerStatus(2);
            hs.setRestOfScrambleBuff(rand2);
            hs.write(this);
        }
    }


    private int getServerCapabilities() {
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
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        flag |= Capabilities.CLIENT_CONNECT_ATTRS;
        return flag;
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
