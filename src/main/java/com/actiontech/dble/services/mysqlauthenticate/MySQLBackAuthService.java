package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.factorys.BusinessServiceFactory;
import com.actiontech.dble.singleton.CapClientFoundRows;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.actiontech.dble.config.ErrorCode.ER_ACCESS_DENIED_ERROR;

/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService extends AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private final AtomicBoolean isHandling = new AtomicBoolean(false);
    private volatile String user;
    private volatile String schema;
    private volatile String passwd;
    private volatile PooledConnectionListener listener;
    private volatile ResponseHandler handler;

    private volatile long serverCapabilities;

    public MySQLBackAuthService(AbstractConnection connection, String user, String schema, String passwd, PooledConnectionListener listener, ResponseHandler handler) {
        super(connection);
        this.user = user;
        this.schema = schema;
        this.passwd = passwd;
        this.listener = listener;
        this.handler = handler;
    }

    // only for com_change_user
    public MySQLBackAuthService(AbstractConnection connection, String user, String passwd, ResponseHandler handler) {
        super(connection);
        this.user = user;
        this.passwd = passwd;
        this.handler = handler;
        // fake for skipping handshake
        ((PooledConnection) connection).setOldSchema(null);
        this.seed = new byte[0];
    }

    @Override
    protected void handleInnerData(byte[] data) {
        try {
            // first,need a seed
            if (this.seed == null) {
                handleHandshake(data);
                return;
            }

            if (authSwitchMore) {
                if (PasswordAuthPlugin.checkPubicKey(data)) {
                    // get the public from the mysql, use the public key to send the new auth pass
                    authSwitchMore = false;
                    BinaryPacket binPacket = new BinaryPacket();
                    byte[] publicKey = binPacket.readKey(data);
                    byte[] authResponse = PasswordAuthPlugin.sendEnPasswordWithPublicKey(seed, publicKey, passwd, ++data[3]);
                    connection.write(authResponse);
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
            LOGGER.warn(e.getMessage(), e);
            onConnectFailed(e);
        } finally {
            synchronized (this) {
                currentTask = null;
            }
        }
    }

    protected void handleInnerData() {
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
            handleInnerData(task.getOrgData());
        }
        //threadUsageStat end
        if (workUsage != null && threadName.startsWith("backend")) {
            workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
        }
    }

    private void handleHandshake(byte[] data) {
        HandshakeV10Packet handshakePacket = new HandshakeV10Packet();
        handshakePacket.read(data);

        ((BackendConnection) connection).setThreadId(handshakePacket.getThreadId());
        connection.initCharacterSet(SystemConfig.getInstance().getCharset());

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
            sendAuthPacket();
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
            String authPluginErrorMessage = "Client don't support the password plugin " + serverPlugin + ",please check the default auth Plugin";
            throw new RuntimeException(authPluginErrorMessage);
        }
    }

    private void sendAuthPacket() throws NoSuchAlgorithmException {
        AuthPacket packet = new AuthPacket();
        packet.setPacketId(nextPacketId());
        packet.setMaxPacketSize(connection.getMaxPacketSize());
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
            connection.setService(BusinessServiceFactory.getBackendBusinessService(info, connection));
            ((BackendConnection) connection).getBackendService().setResponseHandler(handler);
            boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & serverCapabilities);
            boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
            if (clientCompress && usingCompress) {
                connection.getService().setSupportCompress(true);
            }
            if (listener != null) {
                listener.onCreateSuccess((PooledConnection) connection);
            } else if (handler != null) {
                handler.connectionAcquired((BackendConnection) connection);
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
    public void taskToTotalQueue(ServiceTask task) {
        Executor executor = DbleServer.getInstance().getBackendBusinessExecutor();
        if (isHandling.compareAndSet(false, true)) {
            executor.execute(() -> {
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
            });
        }
    }

    @Override
    public void onConnectFailed(Throwable e) {
        if (listener != null) {
            listener.onCreateFail((PooledConnection) connection, e);
        } else if (handler != null) {
            handler.connectionError(e, null);
        }
    }

    private void handleDataError(Exception e) {
        LOGGER.info(this.toString() + " handle data error:", e);
        while (taskQueue.size() > 0) {
            taskQueue.clear();
        }
        connection.close("handle data error:" + e.getMessage());
        if (listener != null) {
            listener.onCreateFail((BackendConnection) connection, e);
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

}
