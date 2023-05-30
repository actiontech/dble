/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.VariationSQLException;
import com.actiontech.dble.btrace.provider.DbleThreadPoolProvider;
import com.actiontech.dble.buffer.BufferPoolRecord;
import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.executor.ThreadContext;
import com.actiontech.dble.net.executor.ThreadPoolStatistic;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.ClusterManageHandler;
import com.actiontech.dble.singleton.ConnectionSerializableLock;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.DelayService;
import com.actiontech.dble.util.DelayServiceControl;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.exception.DirectPrintException;
import com.actiontech.dble.util.exception.NeedDelayedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class FrontendService<T extends UserConfig> extends AbstractService {
    private static final Logger LOGGER = LogManager.getLogger(FrontendService.class);
    protected final AtomicInteger packetId;
    private final Queue<ServiceTask> taskQueue = new PriorityQueue<>();
    // will non null if is dong task
    private volatile Long doingTaskThread = null;
    private AtomicLong taskId = new AtomicLong(1);
    // current task index,Will increased when every new task is processed。
    private long currentTaskIndex = 0;
    // consumed task  id,Used to indicate next task id.(this=nextTaskId-1)
    private volatile long consumedTaskId = 0;
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
    private final DelayService clusterDelayService = new DelayService(() -> ClusterConfig.getInstance().isClusterEnable() && ClusterGeneralConfig.getInstance().isNeedBlocked(), () -> ClusterManageHandler.isDetached() ? "cluster is detached, you should attach cluster first." : null);
    private boolean needDelayed = false;
    protected final ConnectionSerializableLock connectionSerializableLock = new ConnectionSerializableLock(connection.getId(), this);

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

    public DelayService getClusterDelayService() {
        return clusterDelayService;
    }

    public long getConsumedTaskId() {
        return consumedTaskId;
    }

    /**
     * current thread is io thread, so put the task to business executor
     *
     * @param task task
     */
    @Override
    public void handle(ServiceTask task) {
        beforeInsertServiceTask(task);
        task.setTaskId(taskId.getAndIncrement());
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }

    @Override
    public void execute(ServiceTask task, ThreadContext threadContext) {


        if (connection.isClosed()) {
            LOGGER.trace("Discard message in closed {}", task);
            return;
        }

        final PickTaskAlgorithm algorithm = new PickTaskAlgorithm(task);
        try {


            do {
                ServiceTask executeTask = algorithm.pickTask();
                if (executeTask == null) {
                    return;
                }

                try {

                    threadContext.setDoingTask(true);
                    try {
                        DbleThreadPoolProvider.beginProcessFrontBusinessTask();
                        consumeSingleTask(executeTask);
                    } finally {
                        threadContext.setDoingTask(false);
                    }
                } catch (NeedDelayedException e) {
                    needDelayed = true;
                }
                algorithm.afterConsumed(executeTask);
            } while (true);
        } catch (Throwable e) {
            LOGGER.error("frontExecutor process error: ", e);
            connectionSerializableLock.unLock();
            connection.close("frontExecutor process error");
        } finally {
            algorithm.cleanUp();
        }

    }

    protected boolean isNeedDelayConsumeTask() {
        if (clusterDelayService.isNeedDelay()) {
            return true;
        }
        return false;
    }

    public long getCurrentTaskIndex() {
        return currentTaskIndex;
    }

    public boolean isDoingTask() {
        return doingTaskThread != null;
    }

    public void checkMaxPacketSize(byte[] data) {
    }

    @Override
    public void consumeSingleTask(ServiceTask serviceTask) {
        try {
            if (serviceTask.getType() == ServiceTaskType.NORMAL) {
                NormalServiceTask executeTask = (NormalServiceTask) serviceTask;
                if (!executeTask.isReuse()) {
                    this.setPacketId(executeTask.getLastSequenceId());
                }
                checkMaxPacketSize(executeTask.getOrgData());
            }

            super.consumeSingleTask(serviceTask);
            ThreadPoolStatistic.getFrontBusiness().getCompletedTaskCount().increment();
        } catch (NeedDelayedException e) {
            throw e;
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


    /**
     * for multi statement
     *
     * @param packetData packet data
     */
    protected void taskMultiQueryCreate(byte[] packetData) {
        final ServiceTask task = new NormalServiceTask(packetData, this, true);
        //high priority;
        task.setTaskId(-1);
        synchronized (connectionSerializableLock) {
            taskQueue.add(task);
        }
    }

    @Override
    protected abstract void handleInnerData(byte[] data);


    private void taskToLocalQueue(ServiceTask task) {
        if (task == null) {
            throw new IllegalStateException("using null task is illegal");
        }
        taskQueue.offer(task);
    }

    public void notifyTaskThread() {
        DbleServer.getInstance().getFrontHandlerQueue().offerFirst(new NotificationServiceTask(this));
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
        OkPacket ok = OkPacket.getDefault();
        byte packet = (byte) this.packetId.incrementAndGet();
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
        if (e instanceof NeedDelayedException) {
            throw (NeedDelayedException) e;
        } else if (e instanceof DirectPrintException) {
            String msg = e.getMessage();
            writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
        } else if (e instanceof VariationSQLException) {
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

    @Override
    public void afterWriteFinish(@NotNull EnumSet<WriteFlag> writeFlags) {
        clusterDelayService.markDone();
        connectionSerializableLock.unLock();
    }


    @Override
    public BufferPoolRecord.Builder generateBufferRecordBuilder() {
        return BufferPoolRecord.builder().withSql(executeSql);
    }


    private class PickTaskAlgorithm {
        final long currentThreadId = Thread.currentThread().getId();
        boolean isHandleByCurrentThread = false;
        ServiceTask lastConsumedTask = null;
        ServiceTask currentTask;

        PickTaskAlgorithm(@Nonnull ServiceTask currentTask) {
            this.currentTask = currentTask;
        }


        public ServiceTask pickTask() {
            ServiceTask executeTask = null;
            synchronized (connectionSerializableLock) {
                executeTask = innerPickTask();
                if (executeTask == null) {
                    releaseHandle(currentThreadId);
                    return null;
                }
                return executeTask;
            }

        }


        @Nullable
        private ServiceTask innerPickTask() {
            ServiceTask executeTask;
            try {

                if (currentTask != null) {
                    if (!currentTask.getType().equals(ServiceTaskType.NOTIFICATION)) {
                        taskToLocalQueue(currentTask);
                    }
                    //make sure only enqueue once
                    currentTask = null;
                }

                if (lastConsumedTask != null) {
                    if (lastConsumedTask.getType().equals(ServiceTaskType.DELAYED)) {
                        taskToLocalQueue(lastConsumedTask);
                        if (lastConsumedTask.getTaskId() > 0) {
                            //rollback
                            consumedTaskId--;
                        }
                    }

                    lastConsumedTask = null;
                }


                if (doingTaskThread == null) {
                    doingTaskThread = currentThreadId;
                    isHandleByCurrentThread = true;
                } else if (doingTaskThread != currentThreadId) {
                    //this service is handled by another thread
                    return null;
                }


                executeTask = taskQueue.peek();
                if (executeTask == null) {
                    //consumed all.
                    return null;
                }

                if (executeTask.getType().equals(ServiceTaskType.DELAYED)) {
                    //need delay.
                    if (DelayServiceControl.getInstance().blockServiceIfNeed(FrontendService.this, FrontendService.this::isNeedDelayConsumeTask)) {
                        return null;
                    } else {
                        executeTask = ((DelayedServiceTask) executeTask).getOriginTask();
                    }
                }

                //taskId <=0 used for custom event with high priority. task id> 0 must be ordered.
                if (executeTask.getTaskId() > 0 && executeTask.getTaskId() != nextConsumedTaskId()) {
                    //out of order, Probably because the new data is processed slightly faster than the old data. just release and wait next turn.
                    return null;
                }

                if (connectionSerializableLock.isLocking()) {
                    //return  if it is locking .will create notify task when unlock.
                    return null;
                }


                //begin consume now.
                taskQueue.poll();
                currentTaskIndex++;
                if (executeTask.getTaskId() > 0) {
                    consumedTaskId = nextConsumedTaskId();
                }


            } catch (Exception e) {
                LOGGER.error("", e);
                return null;
            }
            return executeTask;
        }

        public void afterConsumed(ServiceTask executeTask) {
            if (needDelayed) {
                beforeWriteFinish(WriteFlags.QUERY_END, ResultFlag.OTHER);
                afterWriteFinish(WriteFlags.QUERY_END);
                lastConsumedTask = new DelayedServiceTask(executeTask);
                needDelayed = false;
            } else {
                lastConsumedTask = executeTask;
            }
        }

        public void cleanUp() {
            //must use synchronized to make sure  task safety added when other thread is doing return.
            if (isHandleByCurrentThread && doingTaskThread != null) {
                synchronized (connectionSerializableLock) {
                    releaseHandle(currentThreadId);
                }
            }
        }

        private void releaseHandle(long currentThreadIdTmp) {
            if (doingTaskThread != null && doingTaskThread == currentThreadIdTmp) {
                doingTaskThread = null;
            }
        }


        private long nextConsumedTaskId() {
            return consumedTaskId + 1;
        }

    }
}
