/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class FrontendService extends VariablesService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendService.class);

    // current schema
    protected volatile String schema;
    // login user
    protected UserName user;
    protected UserConfig userConfig;

    protected long clientCapabilities;
    // sql
    protected volatile String executeSql;
    // received mysql packet
    private ServiceTask currentTask = null;
    private final ConcurrentLinkedQueue<ServiceTask> taskQueue;

    public FrontendService(AbstractConnection connection) {
        super(connection);
        this.taskQueue = new ConcurrentLinkedQueue<>();
    }

    public void initFromAuthInfo(AuthResultInfo info) {
        AuthPacket auth = info.getMysqlAuthPacket();
        clientCapabilities = auth.getClientFlags();
        this.user = new UserName(auth.getUser(), auth.getTenant());
        this.schema = info.getMysqlAuthPacket().getDatabase();
        this.userConfig = info.getUserConfig();
        this.connection.initCharsetIndex(info.getMysqlAuthPacket().getCharsetIndex());
        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            this.setSupportCompress(true);
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(this).append('\'').append(auth.getUser()).append("' login success");
            byte[] extra = auth.getExtra();
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.debug(s.toString());
        }
    }

    /**
     * current thread is io thread, so put the task to business executor
     *
     * @param task task
     */
    @Override
    public void handleTask(ServiceTask task) {
        beforeHandlingTask();
        taskQueue.offer(task);
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    public void execute(ServiceTask task) {
        ServiceTask executeTask = null;
        if (connection.isClosed()) {
            return;
        }

        synchronized (this) {
            if (currentTask == null) {
                executeTask = taskQueue.poll();
                if (executeTask != null) {
                    currentTask = executeTask;
                }
            }
            if (currentTask != task) {
                taskToPriorityQueue(task);
            }
        }

        if (executeTask != null) {
            byte[] data = executeTask.getOrgData();
            if (data != null && !executeTask.isReuse()) {
                this.setPacketId(data[3]);
            }

            this.handleInnerData(data);
            synchronized (this) {
                currentTask = null;
            }
        }
    }

    protected void taskMultiQueryCreate(byte[] packetData) {
        beforeHandlingTask();
        ServiceTask task = new ServiceTask(packetData, this, true);
        taskQueue.offer(task);
    }

    protected abstract void handleInnerData(byte[] data);

    protected void beforeHandlingTask() {
    }

    private void taskToPriorityQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontPriorityQueue().offer(task);
        DbleServer.getInstance().getFrontHandlerQueue().offer(new ServiceTask(null, null));
    }

    @Override
    public void cleanup() {
        synchronized (this) {
            this.currentTask = null;
        }
        this.taskQueue.clear();
        TraceManager.sessionFinish(this);
    }

    public void userConnectionCount() {
        FrontendUserManager.getInstance().countDown(user, this instanceof ManagerService);
    }

    // current schema
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public UserName getUser() {
        return user;
    }

    public UserConfig getUserConfig() {
        return userConfig;
    }

    public String getExecuteSql() {
        return executeSql;
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
    }

    public long getClientCapabilities() {
        return clientCapabilities;
    }

    public void killAndClose(String reason) {
    }

}
