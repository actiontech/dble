/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.VariationSQLException;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.ConnectionSerializableLock;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FrontendService<T extends UserConfig> extends AbstractService {
    private static final Logger LOGGER = LogManager.getLogger(FrontendService.class);
    private final AtomicInteger packetId;
    private final Queue<ServiceTask> taskQueue = new PriorityQueue<>();
    private volatile Long doingTaskThread = null;
    private long taskId = 1;
    private long consumedTaskId = 0;
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
    protected final ConnectionSerializableLock connectionSerializableLock = new ConnectionSerializableLock(connection.getId());

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
        task.setTaskId(taskId++);
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    public void execute(ServiceTask task) {


        if (connection.isClosed()) {
            LOGGER.trace("Discard message in closed {}", task);
            return;
        }


        final long currentThreadId = Thread.currentThread().getId();
        boolean isHandleByCurrentThread = false;
        try {


            do {
                ServiceTask executeTask = null;

                synchronized (connectionSerializableLock) {
                    try {

                        if (task != null && !task.getType().equals(ServiceTaskType.NOTIFICATION)) {
                            taskToLocalQueue(task);
                            //make sure only enqueue once
                            task = null;
                        }

                        if (doingTaskThread == null) {
                            doingTaskThread = currentThreadId;
                            isHandleByCurrentThread = true;
                        } else if (doingTaskThread != currentThreadId) {
                            //this service is handled by another thread
                            return;
                        }


                        executeTask = taskQueue.peek();
                        if (executeTask == null) {
                            //consumed all.
                            releaseHandle(currentThreadId);
                            return;
                        }
                        //taskId <=0 used for custom event with high priority. task id> 0 must be ordered.
                        if (executeTask.getTaskId() > 0 && executeTask.getTaskId() != nextConsumedTaskId()) {
                            //out of order, Probably because the new data is processed slightly faster than the old data. just release and wait next turn.
                            releaseHandle(currentThreadId);
                            return;
                        }

                        if (connectionSerializableLock.isLocking()) {
                            connectionSerializableLock.addListener(this::createNotifyTask);
                            //return true when it was locking and register listener successful.
                            releaseHandle(currentThreadId);
                            return;
                        }


                        //begin consume now.
                        taskQueue.poll();
                        if (executeTask.getTaskId() == nextConsumedTaskId()) {
                            consumedTaskId = nextConsumedTaskId();
                        }


                    } catch (Exception e) {
                        LOGGER.error("", e);
                        releaseHandle(currentThreadId);
                        return;
                    }

                }

                consumeSingleTask(executeTask);
            } while (true);
        } catch (Throwable e) {
            LOGGER.error("frontExecutor process error: ", e);
            connectionSerializableLock.unLock();
        } finally {
            //must use synchronized to make sure  task safety added when other thread is doing return.
            if (isHandleByCurrentThread && doingTaskThread != null) {
                synchronized (connectionSerializableLock) {
                    releaseHandle(currentThreadId);
                }
            }
        }

    }

    @Override
    public void consumeSingleTask(ServiceTask serviceTask) {
        try {
            if (serviceTask.getType() == ServiceTaskType.NORMAL) {
                NormalServiceTask executeTask = (NormalServiceTask) serviceTask;
                byte[] data = executeTask.getOrgData();
                if (data != null && !executeTask.isReuse()) {
                    this.setPacketId(executeTask.getLastSequenceId());
                }
            }

            super.consumeSingleTask(serviceTask);
        } catch (Throwable e) {
            connectionSerializableLock.unLock();
            String msg = e.getMessage();
            if (StringUtil.isEmpty(msg)) {
                LOGGER.warn("Maybe occur a bug, please check it.", e);
                msg = e.toString();
            } else {
                LOGGER.warn("There is an error you may need know.", e);
            }
            writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }

    public int getRecvTaskQueueSize() {
        synchronized (connectionSerializableLock) {
            return taskQueue.size();
        }
    }

    private long nextConsumedTaskId() {
        return consumedTaskId + 1;
    }

    private void releaseHandle(long currentThreadId) {
        if (doingTaskThread != null && doingTaskThread == currentThreadId) {
            doingTaskThread = null;
        }
    }

    void createNotifyTask() {
        taskToPriorityQueue(new NotificationServiceTask(this));
    }

    /**
     * for multi statement
     *
     * @param packetData packet data
     */
    protected void taskMultiQueryCreate(byte[] packetData) {
        final ServiceTask task = new NormalServiceTask(packetData, this, true);
        //high priority;
        task.setTaskId(-1);
        taskQueue.add(task);
        createNotifyTask();
    }

    @Override
    protected abstract void handleInnerData(byte[] data);

    protected void beforeHandlingTask() {
        // ignore
    }

    private void taskToLocalQueue(ServiceTask task) {
        if (task == null) {
            throw new IllegalStateException("using null task is illegal");
        }
        taskQueue.offer(task);
    }

    private void taskToPriorityQueue(ServiceTask task) {
        if (task == null) {
            throw new IllegalStateException("null task is illegal");
        }
        DbleServer.getInstance().getFrontHandlerQueue().offerFirst(task);
    }

    @Override
    public void cleanup() {
        synchronized (connectionSerializableLock) {
            taskQueue.clear();
        }
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

    public ConnectionSerializableLock getConnectionSerializableLock() {
        return connectionSerializableLock;
    }

    public void executeException(Exception e, String sql) {
        sql = sql.length() > 1024 ? sql.substring(0, 1024) + "..." : sql;
        if (e instanceof VariationSQLException) {
            ((VariationSQLException) e).getSendData().write(getConnection());
        } else if (e instanceof SQLException) {
            SQLException sqlException = (SQLException) e;
            String msg = sqlException.getMessage();
            StringBuilder s = new StringBuilder();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(s.append(this).append(sql).toString() + " err:" + msg);
            }
            int vendorCode = sqlException.getErrorCode() == 0 ? ErrorCode.ER_PARSE_ERROR : sqlException.getErrorCode();
            String sqlState = StringUtil.isEmpty(sqlException.getSQLState()) ? "HY000" : sqlException.getSQLState();
            String errorMsg = msg == null ? sqlException.getClass().getSimpleName() : msg;
            writeErrMessage(sqlState, errorMsg, vendorCode);
        } else {
            StringBuilder s = new StringBuilder();
            LOGGER.info(s.append(this).append(sql).toString() + " err:" + e.toString(), e);
            String msg = e.getMessage();
            writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
        }
    }

}
