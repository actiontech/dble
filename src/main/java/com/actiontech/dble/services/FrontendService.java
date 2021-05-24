/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FrontendService<T extends UserConfig> extends AbstractService {
    private static final Logger LOGGER = LogManager.getLogger(FrontendService.class);
    private ServiceTask currentTask = null;
    private final AtomicInteger packetId;
    private final BlockingQueue<ServiceTask> taskQueue = new LinkedBlockingQueue<>(2000);

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
        this.packetId = new AtomicInteger(0);
        this.clientCapabilities = 0;
    }

    public FrontendService(AbstractConnection connection, AuthResultInfo info) {
        super(connection);
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
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    public void execute(ServiceTask task) {

        if (connection.isClosed()) {
            // prevents QUIT、CLOSE_STMT from losing cumulative
            if (task.getOrgData().length > 4 && (task.getOrgData()[4] == MySQLPacket.COM_QUIT || task.getOrgData()[4] == MySQLPacket.COM_STMT_CLOSE)) {
                this.handleInnerData(task.getOrgData());
            }
            return;
        }

        ServiceTask executeTask = null;
        synchronized (this) {
            if (currentTask != null) {
                //currentTask is executing.
                taskToPriorityQueue(task);
                return;
            }

            executeTask = taskQueue.peek();
            if (executeTask == null) {
                return;
            }
            if (executeTask != task) {
                //out of order,adjust it.
                taskToPriorityQueue(task);
                return;
            }
            //drop head task of the queue
            taskQueue.poll();
            currentTask = executeTask;
        }

        try {

            byte[] data = executeTask.getOrgData();
            if (data != null && !executeTask.isReuse()) {
                this.setPacketId(executeTask.getLastSequenceId());
            }

            this.handleInnerData(data);

        } catch (Throwable e) {
            LOGGER.error("process task error", e);
            writeErrMessage(ErrorCode.ER_YES, "process task error, exception is " + e);
            connection.close("process task error");
        } finally {
            synchronized (this) {
                currentTask = null;
            }
        }


    }

    /**
     * for multi statement
     *
     * @param packetData packet data
     */
    protected void taskMultiQueryCreate(byte[] packetData) {
        handle(new ServiceTask(packetData, this, true));
    }

    protected abstract void handleInnerData(byte[] data);

    protected void beforeHandlingTask() {
        // ignore
    }

    private void taskToPriorityQueue(ServiceTask task) {
        if (task == null) {
            throw new IllegalStateException("using null task is illegal");
        }
        DbleServer.getInstance().getFrontPriorityQueue().offer(task);
    }

    @Override
    public void cleanup() {
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
        Optional.ofNullable(StatisticListener.getInstance().getRecorder(this)).ifPresent(r -> r.onFrontendSqlClose());
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
