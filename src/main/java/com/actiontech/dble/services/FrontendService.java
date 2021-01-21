/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FrontendService<T extends UserConfig> extends AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendService.class);

    private ServiceTask currentTask = null;
    private final AtomicInteger packetId;
    private final ConcurrentLinkedQueue<ServiceTask> taskQueue;

    // client capabilities
    private final long clientCapabilities;
    protected volatile byte[] seed;
    // current schema
    protected volatile String schema;
    // login user and config
    protected volatile UserName user;
    protected volatile T userConfig;
    // last execute sql
    protected volatile String executeSql;

    public FrontendService(AbstractConnection connection) {
        super(connection);
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.packetId = new AtomicInteger(0);
        this.clientCapabilities = 0;
    }

    public FrontendService(AbstractConnection connection, AuthResultInfo info) {
        super(connection);
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.packetId = new AtomicInteger(0);
        this.user = info.getUser();
        this.userConfig = (T) info.getUserConfig();
        AuthPacket auth = info.getMysqlAuthPacket();
        this.schema = auth.getDatabase();
        this.clientCapabilities = auth.getClientFlags();
        // initial from auth packet
        this.initCharsetIndex(auth.getCharsetIndex());
        this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        this.autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.multiStatementAllow = auth.isMultStatementAllow();
    }

    /**
     * current thread is io thread, so put the task to business executor
     *
     * @param task task
     */
    @Override
    public void handle(ServiceTask task) {
        beforeHandlingTask();
        taskQueue.offer(task);
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    public void execute(ServiceTask task) {
        if (connection.isClosed()) {
            return;
        }

        ServiceTask executeTask = null;
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
        // ignore
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
        if (user != null) {
            FrontendUserManager.getInstance().countDown(user, this instanceof ManagerService);
        }
    }

    public String getExecuteSql() {
        return executeSql;
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
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

    public void setUser(UserName user) {
        this.user = user;
    }

    public T getUserConfig() {
        return userConfig;
    }

    public void setUserConfig(UserConfig userConfig) {
        this.userConfig = (T) userConfig;
    }

    public int nextPacketId() {
        return packetId.incrementAndGet();
    }

    public void setPacketId(int packetId) {
        this.packetId.set(packetId);
    }

    public byte[] getSeed() {
        return seed;
    }

    public void setSeed(byte[] seed) {
        this.seed = seed;
    }

    public AtomicInteger getPacketId() {
        return packetId;
    }

    public long getClientCapabilities() {
        return clientCapabilities;
    }

    // write
    public void writeOkPacket() {
        OkPacket ok = new OkPacket();
        byte packet = (byte) this.packetId.incrementAndGet();
        ok.read(OkPacket.OK);
        ok.setPacketId(packet);
        write(ok);
    }

    public void writeErrMessage(String code, String msg, int vendorCode) {
        writeErrMessage((byte) this.nextPacketId(), vendorCode, code, msg);
    }

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage((byte) this.nextPacketId(), vendorCode, msg);
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
        err.write(connection);
    }

    public void killAndClose(String reason) {
    }

}
