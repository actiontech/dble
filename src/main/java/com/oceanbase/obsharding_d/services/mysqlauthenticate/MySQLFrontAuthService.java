/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.mysqlauthenticate;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.btrace.provider.GeneralProvider;
import com.oceanbase.obsharding_d.buffer.BufferPoolRecord;
import com.oceanbase.obsharding_d.config.Capabilities;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.Versions;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.user.ManagerUserConfig;
import com.oceanbase.obsharding_d.log.general.GeneralLogHelper;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.*;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.factorys.BusinessServiceFactory;
import com.oceanbase.obsharding_d.services.mysqlauthenticate.util.AuthUtil;
import com.oceanbase.obsharding_d.singleton.CapClientFoundRows;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Future;

import static com.oceanbase.obsharding_d.services.mysqlauthenticate.PluginName.caching_sha2_password;
import static com.oceanbase.obsharding_d.services.mysqlauthenticate.PluginName.mysql_native_password;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLFrontAuthService extends FrontendService implements AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontAuthService.class);
    private volatile boolean receivedMessage = false;

    private static final PluginName[] MYSQL_DEFAULT_PLUGIN = {mysql_native_password, mysql_native_password, mysql_native_password, caching_sha2_password};

    private volatile AuthPacket authPacket;
    private volatile boolean needAuthSwitched;
    private volatile PluginName pluginName;
    private volatile Future<?> asyncLogin;

    public MySQLFrontAuthService(AbstractConnection connection) {
        super(connection);
        this.pluginName = getDefaultPluginName();
    }

    @Override
    public void register() throws IOException {
        greeting();
        this.connection.getSocketWR().asyncRead();
    }

    public void consumeSingleTask(ServiceTask serviceTask) {
        //The close packet can't be filtered
        if (beforeHandlingTask(serviceTask) || (serviceTask.getType() == ServiceTaskType.CLOSE)) {
            if (serviceTask.getType() == ServiceTaskType.NORMAL) {
                final byte[] data = ((NormalServiceTask) serviceTask).getOrgData();
                handleInnerData(data);
            } else if (serviceTask.getType() == ServiceTaskType.SSL) {
                final byte[] data = ((SSLProtoServerTask) serviceTask).getOrgData();
                handleSSLProtoData(data);
            } else {
                handleSpecialInnerData((InnerServiceTask) serviceTask);
            }
        }
        afterDispatchTask(serviceTask);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        receivedMessage = true;
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "handle-auth-data");
        try {
            this.setPacketId(data[3]);
            if (data.length == QuitPacket.QUIT.length && data[4] == MySQLPacket.COM_QUIT) {
                connection.close("quit packet");
                return;
            } else if (data.length == PingPacket.PING.length && data[4] == PingPacket.COM_PING) {
                pingResponse();
                return;
            }
        } catch (Exception e) {
            LOGGER.error("illegal auth packet {}", data, e);
            writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "illegal auth packet, the detail error message is " + e.getMessage());
            connection.close("illegal auth packet");
            return;
        }
        asyncLogin = OBsharding_DServer.getInstance().getComplexQueryExecutor().submit(() -> {
            try {
                GeneralProvider.beforeAuthSuccess();
                if (needAuthSwitched) {
                    handleSwitchResponse(data);
                } else {
                    handleAuthPacket(data);
                }
            } catch (Exception e) {
                if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
                    // print nothing
                } else {
                    LOGGER.error("illegal auth {}", data, e);
                    writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "illegal auth , the detail error message is " + e.getMessage());
                    connection.close("illegal auth");
                }
            } finally {
                TraceManager.finishSpan(this, traceObject);
            }
        });


    }

    private void handleSSLProtoData(byte[] data) {
        ((FrontendConnection) connection).doSSLHandShake(data);
    }

    private void pingResponse() {
        if (OBsharding_DServer.getInstance().isOnline()) {
            OkPacket okPacket = new OkPacket();
            okPacket.setPacketId(2);
            this.write(okPacket);
        } else {
            ErrorPacket errPacket = new ErrorPacket();
            errPacket.setErrNo(ErrorCode.ER_YES);
            errPacket.setMessage("server is offline.".getBytes());
            //close the mysql connection if error occur
            errPacket.setPacketId(2);
            this.write(errPacket);
        }
    }

    private void greeting() {
        // generate auth data
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // save auth data
        byte[] rand = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, rand, 0, rand1.length);
        System.arraycopy(rand2, 0, rand, rand1.length, rand2.length);
        this.seed = rand;

        HandshakeV10Packet hs = new HandshakeV10Packet();
        hs.setPacketId(0);
        hs.setProtocolVersion(Versions.PROTOCOL_VERSION);  // [0a] protocol version   V10
        hs.setServerVersion(Versions.getServerVersion());
        hs.setThreadId(connection.getId());
        hs.setSeed(rand1);
        hs.setServerCapabilities(getServerCapabilities());
        int charsetIndex = CharsetUtil.getCharsetDefaultIndex(SystemConfig.getInstance().getCharset());
        hs.setServerCharsetIndex((byte) (charsetIndex & 0xff));
        hs.setServerStatus(2);
        hs.setRestOfScrambleBuff(rand2);
        hs.setAuthPluginName(pluginName.name().getBytes());

        //writeDirectly out
        hs.write(connection);
    }

    private void handleAuthPacket(byte[] data) {
        AuthPacket auth = new AuthPacket();
        auth.read(data);
        if (connection.isRequestSSL() == null) {
            /*
            ++++ Only need to be based on the first CLIENT_SSL value ++++
            + Login request will be sent twice during ssl
            + 1. before the client hello and does not contain account password and other information
            + 2. encrypted after SSL authentication and contains account password and other information
             */
            connection.setRequestSSL(auth.getIsSSLRequest());
        }
        if (auth.getIsSSLRequest())
            return;

        this.authPacket = auth;
        try {
            if (null != auth.getAuthPlugin()) {
                PluginName name = PluginName.valueOf(auth.getAuthPlugin());
                if (pluginName != name) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("auth switch request client-plugin:[{}],server-plugin:[{}]->[{}]", name, pluginName, mysql_native_password);
                    }
                    needAuthSwitched = true;
                    this.pluginName = mysql_native_password;
                    sendSwitchPacket(pluginName);
                    return;
                }
            }
            // check user and password whether is correct
            auth();
        } catch (IllegalArgumentException e) {
            needAuthSwitched = true;
            sendSwitchPacket(pluginName);
        }
    }

    private void sendSwitchPacket(PluginName name) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "request-client-switch");
        try {
            AuthSwitchRequestPackage authSwitch = new AuthSwitchRequestPackage(name.toString().getBytes(), seed);
            authSwitch.setPacketId(this.nextPacketId());
            authSwitch.bufferWrite(connection);
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    private void handleSwitchResponse(byte[] data) {
        AuthSwitchResponsePackage authSwitchResponse = new AuthSwitchResponsePackage();
        authSwitchResponse.read(data);
        authPacket.setPassword(authSwitchResponse.getAuthPluginData());

        // check user and password whether is correct
        auth();
    }

    private void auth() {
        AuthResultInfo info = AuthUtil.auth((FrontendConnection) connection, seed, pluginName, authPacket);
        checkForResult(info);
        boolean isFoundRows = Capabilities.CLIENT_FOUND_ROWS == (Capabilities.CLIENT_FOUND_ROWS & authPacket.getClientFlags());
        if (!(userConfig instanceof ManagerUserConfig) && isFoundRows != CapClientFoundRows.getInstance().isEnableCapClientFoundRows()) {
            LOGGER.warn("the client requested CLIENT_FOUND_ROWS capabilities is '{}', OBsharding-D is configured as '{}',pls set the same.", isFoundRows ? "found rows" : "affect rows", CapClientFoundRows.getInstance().isEnableCapClientFoundRows() ? "found rows" : "affect rows");
        }
    }

    @Override
    public void writeOkPacket() {
        OkPacket ok = OkPacket.getDefault();
        byte packet = (byte) this.packetId.incrementAndGet();
        ok.setPacketId(packet);
        //prevent service change
        write(ok, this);
    }

    private void checkForResult(AuthResultInfo info) {
        TraceManager.serviceTrace(this, "check-auth-result");
        try {
            if (info.isSuccess()) {
                FrontendService service = BusinessServiceFactory.getBusinessService(info, connection);
                // for com_change_user
                service.setSeed(seed);
                connection.setService(service);
                writeOkPacket();
                // must after sending ok packet
                boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & authPacket.getClientFlags());
                boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
                if (clientCompress && usingCompress) {
                    connection.setSupportCompress(true);
                }
                if (LOGGER.isDebugEnabled()) {
                    StringBuilder s = new StringBuilder(40);
                    s.append('\'').append(authPacket.getUser()).append("' login success");
                    byte[] extra = authPacket.getExtra();
                    if (extra != null && extra.length > 0) {
                        s.append(",extra:").append(new String(extra));
                    }
                    LOGGER.debug(s.toString());
                }
                String schema;
                GeneralLogHelper.putGLog(connection.getId(), MySQLPacket.TO_STRING.get(MySQLPacket.COM_CONNECT),
                        info.getUserConfig().getName() + "@" + connection.getHost() +
                                " on " + ((schema = service.getSchema()) == null ? "" : schema) +
                                " using TCP/IP");

            } else {
                this.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, info.getErrorMsg());
            }
        } finally {
            TraceManager.sessionFinish(this);
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
        if (SystemConfig.getInstance().isSupportSSL()) {
            flag |= Capabilities.CLIENT_SSL;
        }
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

    private PluginName getDefaultPluginName() {
        String majorMySQLVersion = SystemConfig.getInstance().getFakeMySQLVersion();
        if (majorMySQLVersion != null) {
            String[] versions = majorMySQLVersion.split("\\.");
            if (versions.length == 3) {
                majorMySQLVersion = versions[0] + "." + versions[1];
                for (int i = 0; i < SystemConfig.MYSQL_VERSIONS.length; i++) {
                    // version is x.y.z ,just compare the x.y
                    if (majorMySQLVersion.equals(SystemConfig.MYSQL_VERSIONS[i])) {
                        return MYSQL_DEFAULT_PLUGIN[i];
                    }
                }
            }
        } else {
            return mysql_native_password;
        }
        return null;
    }

    @Override
    public boolean haveNotReceivedMessage() {
        return !receivedMessage;
    }

    @Override
    public void cleanup() {
        final Future<?> loginFuture = this.asyncLogin;
        if (loginFuture != null && !loginFuture.isDone()) {
            loginFuture.cancel(true);
            this.asyncLogin = null;
        }
        super.cleanup();
    }

    @Override
    public BufferPoolRecord.Builder generateBufferRecordBuilder() {
        return BufferPoolRecord.builder().withSql("<<FRONT>>");
    }
}
