package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.services.factorys.BusinessServiceFactory;
import com.actiontech.dble.services.mysqlauthenticate.plugin.BackendDefaulPlugin;
import com.actiontech.dble.services.mysqlauthenticate.plugin.CachingSHA2Pwd;
import com.actiontech.dble.services.mysqlauthenticate.plugin.MySQLAuthPlugin;
import com.actiontech.dble.services.mysqlauthenticate.plugin.NativePwd;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.actiontech.dble.config.ErrorCode.ER_ACCESS_DENIED_ERROR;


/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService extends MySQLBasedService implements AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private volatile MySQLAuthPlugin plugin;

    protected final AtomicBoolean isHandling = new AtomicBoolean(false);

    private volatile String user;
    private volatile String schema;
    private volatile String passwd;
    private volatile PooledConnectionListener listener;
    private volatile ResponseHandler handler;

    public MySQLBackAuthService(AbstractConnection connection, String user, String schema, String passwd, PooledConnectionListener listener, ResponseHandler handler) {
        super(connection);
        plugin = new BackendDefaulPlugin(connection);
        this.user = user;
        this.schema = schema;
        this.passwd = passwd;
        this.listener = listener;
        this.proto = new MySQLProtoHandlerImpl();
        this.handler = handler;
    }

    @Override
    protected void handleInnerData(byte[] data) {
        try {
            switch (plugin.handleBackData(data)) {
                case caching_sha2_password:
                    this.plugin = new CachingSHA2Pwd(plugin);
                    plugin.authenticate(user, passwd, schema, ++data[3]);
                    break;
                case mysql_native_password:
                    this.plugin = new NativePwd(plugin);
                    plugin.authenticate(user, passwd, schema, ++data[3]);
                    break;
                case plugin_same_with_default:
                    checkForResult();
                    break;
                default:
                    String authPluginErrorMessage = "Client don't support the password plugin ,please check the default auth Plugin";
                    LOGGER.warn(authPluginErrorMessage);
                    throw new RuntimeException(authPluginErrorMessage);
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
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


    public void checkForResult() {
        if (plugin.getInfo() == null) {
            return;
        }
        if (plugin.getInfo().isSuccess()) {
            connection.setService(BusinessServiceFactory.getBackendBusinessService(plugin.getInfo(), connection));
            ((BackendConnection) connection).getBackendService().setResponseHandler(handler);
            boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & plugin.getHandshakePacket().getServerCapabilities());
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
            throw new ConnectionException(ER_ACCESS_DENIED_ERROR, plugin.getInfo().getErrorMsg());
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
        }
        connection.close("handle data error:" + e.getMessage());
        if (listener != null) {
            listener.onCreateFail((BackendConnection) connection, e);
        } else if (handler != null) {
            handler.connectionError(e, null);
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

}
