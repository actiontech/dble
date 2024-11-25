/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.mysqlauthenticate;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.delyDetection.DelayDetectionSqlJob;
import com.oceanbase.obsharding_d.backend.heartbeat.HeartbeatSQLJob;
import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ResponseHandler;
import com.oceanbase.obsharding_d.backend.pool.PooledConnectionListener;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.config.Capabilities;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.ConnectionException;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.AuthResultInfo;
import com.oceanbase.obsharding_d.net.service.AuthService;
import com.oceanbase.obsharding_d.net.service.ServiceTask;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.services.BackendService;
import com.oceanbase.obsharding_d.services.factorys.BusinessServiceFactory;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.singleton.CapClientFoundRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;

import static com.oceanbase.obsharding_d.config.ErrorCode.ER_ACCESS_DENIED_ERROR;

/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService extends BackendService implements AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private final String user;
    private final String passwd;
    private volatile String schema;
    private volatile PooledConnectionListener listener;
    private volatile ResponseHandler handler;
    private volatile byte[] seed;
    private volatile boolean authSwitchMore;
    private volatile PluginName pluginName;
    private volatile long serverCapabilities;
    private volatile boolean highPriority = false;

    public MySQLBackAuthService(BackendConnection connection, String user, String schema, String passwd, PooledConnectionListener listener, ResponseHandler handler) {
        super(connection);
        if (handler instanceof HeartbeatSQLJob || handler instanceof DelayDetectionSqlJob) {
            highPriority = true;
        }
        this.user = user;
        this.schema = schema;
        this.passwd = passwd;
        this.listener = listener;
        this.handler = handler;
    }

    // only for com_change_user
    public MySQLBackAuthService(BackendConnection connection, String user, String passwd, ResponseHandler handler) {
        super(connection);
        if (handler instanceof HeartbeatSQLJob || handler instanceof DelayDetectionSqlJob) {
            highPriority = true;
        }
        this.user = user;
        this.passwd = passwd;
        this.handler = handler;
        // fake for skipping handshake
        connection.setOldSchema(null);
        this.seed = new byte[0];
    }


    @Override
    protected void handleInnerData(byte[] data) {
        try {
            // first,need a seed
            if (this.seed == null) {
                if (data[4] == ErrorPacket.FIELD_COUNT) {
                    ErrorPacket err = new ErrorPacket();
                    err.read(data);
                    throw new RuntimeException(new String(err.getMessage()));
                } else {
                    handleHandshake(data);
                }
                return;
            }

            if (authSwitchMore) {
                if (PasswordAuthPlugin.checkPubicKey(data)) {
                    // get the public from the mysql, use the public key to send the new auth pass
                    authSwitchMore = false;
                    BinaryPacket binPacket = new BinaryPacket();
                    byte[] publicKey = binPacket.readKey(data);
                    byte[] authResponse = PasswordAuthPlugin.sendEnPasswordWithPublicKey(seed, publicKey, passwd, ++data[3]);
                    this.write(authResponse, WriteFlags.QUERY_END);
                    return;
                }
            }

            switch (data[4]) {
                case OkPacket.FIELD_COUNT:
                    // get ok from mysql,login success
                    checkForResult(new AuthResultInfo(null));
                    break;
                case ErrorPacket.FIELD_COUNT:
                    // get error response from the mysql,login be rejected
                    ErrorPacket err = new ErrorPacket();
                    err.read(data);
                    String errMsg = new String(err.getMessage());
                    checkForResult(new AuthResultInfo(errMsg));
                    break;
                case AuthSwitchRequestPackage.STATUS: {
                    //need auth switch for other plugin
                    AuthSwitchRequestPackage authSwitchRequestPackage = new AuthSwitchRequestPackage();
                    authSwitchRequestPackage.read(data);

                    String authPluginName = new String(authSwitchRequestPackage.getAuthPluginName());
                    try {
                        this.pluginName = PluginName.valueOf(authPluginName);
                    } catch (IllegalArgumentException e) {
                        String authPluginErrorMessage = "Client don't support the password plugin " + authPluginName + ",please check the default auth Plugin";
                        throw new RuntimeException(authPluginErrorMessage);
                    }
                    this.seed = authSwitchRequestPackage.getAuthPluginData();
                    sendSwitchResponse(PasswordAuthPlugin.passwd(passwd, seed, pluginName), ++data[3]);
                    break;
                }
                case PasswordAuthPlugin.AUTH_SWITCH_MORE: {
                    authSwitchMore = true;
                    //need auth switch for other plugin
                    if (data.length > 5 && data[5] == PasswordAuthPlugin.AUTHSTAGE_FULL) {
                        sendSwitchResponse(new byte[]{2}, ++data[3]);
                    }
                    break;
                }
                default:
                    break;
            }
        } catch (Exception e) {
            LOGGER.warn(e.toString(), e);
            onConnectFailed(e);
        }
    }

    private void handleHandshake(byte[] data) {
        HandshakeV10Packet handshakePacket = new HandshakeV10Packet();
        handshakePacket.read(data);

        connection.setThreadId(handshakePacket.getThreadId());
        int sl1 = handshakePacket.getSeed().length;
        int sl2 = handshakePacket.getRestOfScrambleBuff().length;
        byte[] seedTemp = new byte[sl1 + sl2];
        System.arraycopy(handshakePacket.getSeed(), 0, seedTemp, 0, sl1);
        System.arraycopy(handshakePacket.getRestOfScrambleBuff(), 0, seedTemp, sl1, sl2);
        this.seed = seedTemp;
        this.serverCapabilities = handshakePacket.getServerCapabilities();

        String serverPlugin = new String(handshakePacket.getAuthPluginName());
        try {
            pluginName = PluginName.valueOf(serverPlugin);
            sendAuthPacket(++data[3]);
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
            String authPluginErrorMessage = "Client don't support the password plugin " + serverPlugin + ",please check the default auth Plugin";
            throw new RuntimeException(authPluginErrorMessage);
        }
    }

    private void sendAuthPacket(byte packetId) throws NoSuchAlgorithmException {
        AuthPacket packet = new AuthPacket();
        packet.setPacketId(packetId);
        packet.setMaxPacketSize(SystemConfig.getInstance().getMaxPacketSize());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        packet.setCharsetIndex(charsetIndex);
        packet.setUser(user);
        packet.setPassword(PasswordAuthPlugin.passwd(passwd, this.seed, pluginName));
        packet.setClientFlags(getClientFlagSha());
        packet.setAuthPlugin(pluginName.name());
        packet.setDatabase(schema);
        packet.bufferWrite(connection);
    }

    private void sendSwitchResponse(byte[] authData, byte packetId) {
        AuthSwitchResponsePackage packet = new AuthSwitchResponsePackage();
        packet.setAuthPluginData(authData);
        packet.setPacketId(packetId);
        packet.bufferWrite(this.connection);
    }

    private void checkForResult(AuthResultInfo info) {
        if (info == null) {
            return;
        }
        if (info.isSuccess()) {
            final MySQLResponseService service = (MySQLResponseService) BusinessServiceFactory.getBackendBusinessService(info, connection);
            service.setResponseHandler(handler);
            // support
            boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & serverCapabilities);
            boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
            if (clientCompress && usingCompress) {
                connection.setSupportCompress(true);
            }
            connection.setService(service);
            if (listener != null) {
                listener.onCreateSuccess(connection);
            } else if (handler != null) {
                handler.connectionAcquired(connection);
            }
        } else {
            throw new ConnectionException(ER_ACCESS_DENIED_ERROR, info.getErrorMsg());
        }
    }

    @Override
    public void register() throws IOException {
        connection.getSocketWR().asyncRead();
    }

    @Override
    protected void doHandle(ServiceTask task) {
        if (SystemConfig.getInstance().getUsePerformanceMode() != 1) {
            super.doHandle(null);
        } else {
            if (task == null) return;
            if (isHandling.compareAndSet(false, true)) {
                OBsharding_DServer.getInstance().getConcurrentBackHandlerQueue().offer(task);
            }
        }
    }

    @Override
    public void onConnectFailed(Throwable e) {
        if (listener != null) {
            listener.onCreateFail(connection, e);
        } else if (handler != null) {
            handler.connectionError(e, null);
        }
    }

    @Override
    protected void handleDataError(Exception e) {
        super.handleDataError(e);
        if (listener != null) {
            listener.onCreateFail(connection, e);
        } else if (handler != null) {
            handler.connectionError(e, null);
        }
    }

    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        boolean isEnableCapClientFoundRows = CapClientFoundRows.getInstance().isEnableCapClientFoundRows();
        if (isEnableCapClientFoundRows) {
            flag |= Capabilities.CLIENT_FOUND_ROWS;
        }
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

    private long getClientFlagSha() {
        int flag = 0;
        flag |= initClientFlags();
        flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        return flag;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (handler != null) {
            this.handler.connectionClose(this, "abnormal connection");
        }
    }

    @Override
    public BufferPoolRecord.Builder generateBufferRecordBuilder() {
        return BufferPoolRecord.builder().withSql("<<BACK>>");
    }

    @Override
    public boolean haveNotReceivedMessage() {
        throw new UnsupportedOperationException();
    }


    protected boolean isSupportFlowControl() {
        return false;
    }

    @Override
    protected Executor getExecutor() {
        if (highPriority) {
            return OBsharding_DServer.getInstance().getComplexQueryExecutor();
        } else {
            return OBsharding_DServer.getInstance().getBackendExecutor();
        }
    }
}
