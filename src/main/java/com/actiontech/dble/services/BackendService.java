/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.DbleThreadPoolProvider;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.config.model.user.AnalysisUserConfig;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.executor.BackendRunnable;
import com.actiontech.dble.net.executor.ThreadContext;
import com.actiontech.dble.net.executor.ThreadContextView;
import com.actiontech.dble.net.executor.ThreadPoolStatistic;
import com.actiontech.dble.net.handler.BackEndCleaner;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.response.ProtocolResponseHandler;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.NormalServiceTask;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.net.service.ServiceTaskType;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.singleton.FlowController;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.exception.UnknownTxIsolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public abstract class BackendService extends AbstractService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackendService.class);

    // received data
    protected final AtomicBoolean isHandling;
    private final ConcurrentLinkedQueue<ServiceTask> taskQueue;

    // protocol response handler
    protected volatile ProtocolResponseHandler protocolResponseHandler;
    protected ProtocolResponseHandler defaultResponseHandler;

    // backend conn
    protected final BackendConnection connection;

    // in case of the connection is closed, the data is remaining
    protected volatile boolean isRowDataFlowing = false;
    protected volatile BackEndCleaner recycler = null;

    // sync context
    protected volatile boolean metaDataSynced = true;
    protected volatile boolean isExecuting;
    protected volatile StatusSync statusSync;

    protected boolean autocommitSynced;
    protected boolean isolationSynced;
    private final BackendOnetimeRunnable taskRunnable = new BackendOnetimeRunnable();

    private final AtomicInteger readSize = new AtomicInteger(0);

    public BackendService(BackendConnection connection) {
        super(connection);
        this.connection = connection;
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.isHandling = new AtomicBoolean(false);
        // variables
        if (connection != null) {
            this.autocommitSynced = connection.getInstance().isAutocommitSynced();
            this.isolationSynced = connection.getInstance().isIsolationSynced();
        }
        initCharacterSet(SystemConfig.getInstance().getCharset());
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            this.txIsolation = -1;
        }
        this.multiStatementAllow = true;
    }

    /**
     * currently, the thread is io thread, so we put the task to thread pool to handle
     *
     * @param task task contains mysql packet
     */
    @Override
    public void handle(ServiceTask task) {
        beforeInsertServiceTask(task);
        productReadingData(task);
        taskQueue.offer(task);
        doHandle(task);
    }

    public int getReadSize() {
        return readSize.get();
    }

    protected void doHandle(ServiceTask task) {
        if (isHandling.compareAndSet(false, true)) {
            Executor executor = getExecutor();
            executor.execute(taskRunnable);
        }
    }


    /**
     * used when Performance Mode
     */
    @Override
    public void execute(ServiceTask task, ThreadContext threadContext) {
        try {
            handleTaskQueue(threadContext);
        } catch (Exception e) {
            handleDataError(e);
        } finally {
            isHandling.set(false);
            if (taskQueue.size() > 0) {
                doHandle(task);
            }
        }
    }

    protected Executor getExecutor() {
        return DbleServer.getInstance().getBackendExecutor();
    }

    private void handleTaskQueue(ThreadContext threadContext) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(this, "loop-handle-back-data");
        try {
            ServiceTask task;
            String threadName = null;
            ThreadWorkUsage workUsage = null;
            long workStart = 0;
            if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
                threadName = Thread.currentThread().getName();
                workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);
                if (threadName.contains(DbleServer.BACKEND_WORKER_NAME)) {
                    if (workUsage == null) {
                        workUsage = new ThreadWorkUsage();
                        DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                    }
                }

                workStart = System.nanoTime();
            }
            threadContext.setDoingTask(true);
            try {
                // handleData
                while ((task = taskQueue.poll()) != null) {
                    DbleThreadPoolProvider.beginProcessBackendBusinessTask();
                    consumeReadingData(task);
                    this.consumeSingleTask(task);
                    ThreadPoolStatistic.getBackendBusiness().getCompletedTaskCount().increment();
                }
            } finally {
                threadContext.setDoingTask(false);
            }

            // threadUsageStat end
            if (workUsage != null && threadName.contains(DbleServer.BACKEND_WORKER_NAME)) {
                workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
            }
        } finally {
            TraceManager.finishSpan(this, traceObject);
        }
    }

    void parseErrorPacket(byte[] data, String reason) {
        try {
            ErrorPacket errPkg = new ErrorPacket();
            errPkg.read(data);
            String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
            LOGGER.warn("drop the unprocessed error packet, reason:{},packet content:{},back service:{},", reason, errMsg, this);

        } catch (RuntimeException e) {
            LOGGER.info("error drop error-packet", e);
        }
    }

    /**
     * handle mysql packet returned from backend mysql
     *
     * @param data mysql binary packet
     */
    @Override
    protected void handleInnerData(byte[] data) {
        if (connection.isClosed()) {
            if (data != null && data.length > 4 && data[4] == ErrorPacket.FIELD_COUNT) {
                parseErrorPacket(data, "connection close");
            }
            return;
        }

        byte type = data[4];
        if (type == OkPacket.FIELD_COUNT) {
            protocolResponseHandler.ok(data);
        } else if (type == ErrorPacket.FIELD_COUNT) {
            protocolResponseHandler.error(data);
        } else if (type == EOFPacket.FIELD_COUNT) {
            protocolResponseHandler.eof(data);
        } else {
            protocolResponseHandler.data(data);
        }
    }

    protected void handleDataError(Exception e) {
        LOGGER.warn(this.toString() + " handle data error:", e);
        connection.close("handle data error:" + e.getMessage());
        while (taskQueue.size() > 0) {
            clearTaskQueue();
            readSize.set(0);
            // clear all data from the client
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        }
    }

    private void clearTaskQueue() {
        ServiceTask task;
        while ((task = taskQueue.poll()) != null) {
            if (task.getType() == ServiceTaskType.NORMAL) {
                final byte[] data = ((NormalServiceTask) task).getOrgData();
                if (data != null && data.length > 4 && data[4] == ErrorPacket.FIELD_COUNT) {
                    parseErrorPacket(data, "cleanup");
                }
            }
        }
    }

    @Override
    public void cleanup() {
        clearTaskQueue();
        readSize.set(0);
        backendSpecialCleanUp();
        TraceManager.sessionFinish(this);
    }

    public void release() {
        if (!metaDataSynced) { // indicate connection not normal finished
            LOGGER.info("can't sure connection syn result,so close it " + this);
            this.connection.businessClose("syn status unknown ");
            return;
        }
        if (this.usrVariables.size() > 0) {
            this.connection.businessClose("close for clear usrVariables");
            return;
        }
        metaDataSynced = true;
        statusSync = null;

        if (innerRelease()) {
            TraceManager.sessionFinish(this);
            connection.getPoolRelated().release(connection);
        }
    }


    private void productReadingData(ServiceTask task) {
        if (needCalcReadingData(task) != null) {
            int currentReadSize = readSize.addAndGet(((NormalServiceTask) task).getOrgData().length);
            if (currentReadSize > connection.getFlowHighLevel()) {
                LOGGER.debug("This backend connection begins flow control, currentReadingSize= {},conn info:{}", currentReadSize, connection);
                connection.disableRead();
            }
        }
    }

    private void consumeReadingData(ServiceTask task) {
        BusinessService businessService;
        if ((businessService = needCalcReadingData(task)) != null) {
            int currentReadSize = readSize.addAndGet(-((NormalServiceTask) task).getOrgData().length);
            if (currentReadSize <= connection.getFlowLowLevel() &&
                    !businessService.isFlowControlled()) {
                LOGGER.debug("This backend connection stop flow control, currentReadingSize= {},conn info:{}", currentReadSize, connection);
                connection.enableRead();
            }
        }
    }

    private BusinessService needCalcReadingData(ServiceTask task) {
        Session session;
        FrontendService frontendService;
        if (FlowController.isEnableFlowControl() &&
                isSupportFlowControl() &&
                task.getType() == ServiceTaskType.NORMAL &&
                (session = ((MySQLResponseService) this).getOriginSession()) != null &&
                (frontendService = session.getSource().getFrontEndService()) != null &&
                frontendService instanceof BusinessService) {
            return (BusinessService) frontendService;
        } else return null;
    }

    protected abstract boolean isSupportFlowControl();

    protected boolean innerRelease() {
        return true;
    }

    // RowDataFlowing
    public boolean isRowDataFlowing() {
        return isRowDataFlowing;
    }

    public void setRowDataFlowing(boolean rowDataFlowing) {
        isRowDataFlowing = rowDataFlowing;
    }


    public void setRecycler(BackEndCleaner recycler) {
        this.recycler = recycler;
    }

    public void backendSpecialCleanUp() {
        isExecuting = false;
        this.releaseSignal();
    }

    public void releaseSignal() {
        isRowDataFlowing = false;
        Optional.ofNullable(recycler).ifPresent(res -> res.signal());
        recycler = null;
    }

    public void resetContextStatus() {
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            this.txIsolation = -1;
        }
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        this.initCharacterSet(SystemConfig.getInstance().getCharset());
        this.usrVariables.clear();
        this.sysVariables.clear();
        this.sysVariables.put("sql_mode", null);
    }

    // sync context
    public boolean syncAndExecute() {
        StatusSync sync = this.statusSync;
        if (sync == null) {
            isExecuting = false;
            return true;
        } else {
            boolean executed = sync.synAndExecuted(this);
            if (executed) {
                isExecuting = false;
                statusSync = null;
            }
            return executed;
        }
    }

    public boolean isExecuting() {
        return isExecuting;
    }

    public void setExecuting(boolean executing) {
        isExecuting = executing;
    }

    protected void addSyncContext() {
        if (statusSync == null) {
            statusSync = new StatusSync(1);
        } else {
            this.statusSync.synCmdCount.incrementAndGet();
        }
    }

    protected StringBuilder getSynSql(boolean expectAutocommit, VariablesService front) {
        // variables
        Set<String> toResetSys = new HashSet<>();
        Map<String, String> usrVariables = front.getUsrVariables();
        Map<String, String> sysVariables = front.getSysVariables();
        String setSql = getSetSQL(usrVariables, sysVariables, toResetSys);
        int setSqlFlag = setSql == null ? 0 : 1;
        // schema
        String schema = connection.getSchema();
        int schemaSyn = StringUtil.equals(schema, connection.getOldSchema()) || schema == null ? 0 : 1;
        // charset
        CharsetNames clientCharset = front.getCharset();
        int charsetSyn = charsetName.equals(clientCharset) ? 0 : 1;
        // txIsolation
        int clientTxIsolation = front.getTxIsolation();
        boolean isReadOnly = front.isReadOnly();
        int txIsolationSyn = this.txIsolation == clientTxIsolation ? 0 : 1;
        int readOnlySyn = this.sessionReadOnly == isReadOnly ? 0 : 1;
        int txAndReadOnlySyn = txIsolationSyn == 0 && readOnlySyn == 0 ? 0 : 1;
        // autocommit
        int autoCommitSyn = (this.autocommit == expectAutocommit) ? 0 : 1;
        int synCount = schemaSyn + charsetSyn + txAndReadOnlySyn + autoCommitSyn + setSqlFlag;
        if (synCount == 0 || ignoreSql(front)) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        if (schemaSyn == 1) {
            getChangeSchemaCommand(sb, schema);
        } else {
            schema = null;
        }
        if (charsetSyn == 1) {
            getCharsetCommand(sb, clientCharset);
        } else {
            clientCharset = null;
        }
        if (txAndReadOnlySyn == 1) {
            sb.append("SET SESSION TRANSACTION ");
            if (txIsolationSyn == 1) {
                getTxIsolationCommand(sb, clientTxIsolation);
            }
            if (readOnlySyn == 1) {
                if (txIsolationSyn == 1) sb.append(",");
                getReadOnlyCommand(sb, isReadOnly);
            }
            sb.append(";");
        }
        if (autoCommitSyn == 1) {
            getAutocommitCommand(sb, expectAutocommit);
        }
        if (setSqlFlag == 1) {
            sb.append(setSql);
        }
        metaDataSynced = false;
        statusSync = new StatusSync(schema,
                clientCharset, clientTxIsolation, expectAutocommit, isReadOnly,
                synCount, usrVariables, sysVariables, toResetSys);
        return sb;
    }

    /**
     * Temporary way,it will be revised in the future
     *
     * @param service
     * @return
     */
    private boolean ignoreSql(VariablesService service) {
        if (service instanceof RWSplitService) {
            return (((RWSplitService) service).getUserConfig() instanceof AnalysisUserConfig);
        } else if (service instanceof MySQLResponseService) {
            return ((MySQLResponseService) service).getConnection().getInstance().getConfig().getDataBaseType() == DataBaseType.CLICKHOUSE;
        }
        return false;
    }

    private void getChangeSchemaCommand(StringBuilder sb, String schema) {
        if (schema != null) {
            sb.append("use `");
            sb.append(schema);
            sb.append("`;");
        }
    }

    private void getCharsetCommand(StringBuilder sb, CharsetNames clientCharset) {
        sb.append("SET CHARACTER_SET_CLIENT = ");
        sb.append(clientCharset.getClient());
        sb.append(",CHARACTER_SET_RESULTS = ");
        sb.append(clientCharset.getResults());
        sb.append(",COLLATION_CONNECTION = ");
        sb.append(clientCharset.getCollation());
        sb.append(";");
    }

    private void getTxIsolationCommand(StringBuilder sb, int txIsolation) {
        switch (txIsolation) {
            case Isolations.READ_UNCOMMITTED:
                sb.append("ISOLATION LEVEL READ UNCOMMITTED");
                return;
            case Isolations.READ_COMMITTED:
                sb.append("ISOLATION LEVEL READ COMMITTED");
                return;
            case Isolations.REPEATABLE_READ:
                sb.append("ISOLATION LEVEL REPEATABLE READ");
                return;
            case Isolations.SERIALIZABLE:
                sb.append("ISOLATION LEVEL SERIALIZABLE");
                return;
            default:
                throw new UnknownTxIsolationException("txIsolation:" + txIsolation);
        }
    }

    private void getReadOnlyCommand(StringBuilder sb, boolean isReadOnly) {
        if (isReadOnly) {
            sb.append("READ ONLY");
        } else {
            sb.append("READ WRITE");
        }
    }

    private void getAutocommitCommand(StringBuilder sb, boolean autoCommit) {
        if (autoCommit) {
            sb.append("SET autocommit=1;");
        } else {
            sb.append("SET autocommit=0;");
        }
    }

    private String getSetSQL(Map<String, String> usrVars, Map<String, String> sysVars, Set<String> toResetSys) {
        // new final var
        List<Pair<String, String>> setVars = new ArrayList<>();
        // tmp add all backend sysVariables
        Map<String, String> tmpSysVars = new HashMap<>(sysVariables);
        // for all front end sysVariables
        for (Map.Entry<String, String> entry : sysVars.entrySet()) {
            if (!tmpSysVars.containsKey(entry.getKey())) {
                setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
            } else {
                String value = tmpSysVars.remove(entry.getKey());
                //if backend is not equal frontend, need to reset
                if (!StringUtil.equalsIgnoreCase(entry.getValue(), value)) {
                    setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
            }
        }
        //tmp now = backend -(backend &&frontend)
        for (Map.Entry<String, String> entry : tmpSysVars.entrySet()) {
            String value = DbleServer.getInstance().getSystemVariables().getDefaultValue(entry.getKey());
            try {
                assert value != null;
                new BigDecimal(value);
            } catch (NumberFormatException e) {
                value = "`" + value + "`";
            }
            setVars.add(new Pair<>(entry.getKey(), value));
            toResetSys.add(entry.getKey());
        }

        for (Map.Entry<String, String> entry : usrVars.entrySet()) {
            if (!usrVariables.containsKey(entry.getKey())) {
                setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
            } else {
                if (!StringUtil.equalsIgnoreCase(entry.getValue(), usrVariables.get(entry.getKey()))) {
                    setVars.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
            }
        }

        if (setVars.size() == 0)
            return null;
        StringBuilder sb = new StringBuilder("set ");
        int cnt = 0;
        for (Pair<String, String> var : setVars) {
            if (cnt > 0) {
                sb.append(",");
            }
            sb.append(var.getKey());
            sb.append("=");
            sb.append(var.getValue());
            cnt++;
        }
        sb.append(";");
        return sb.toString();
    }

    public long getRecvTaskQueueSize() {
        return taskQueue.size();
    }

    private static final class StatusSync {
        private final String schema;
        private final CharsetNames clientCharset;
        private final Integer txIsolation;
        private final Boolean autocommit;
        private final Boolean readOnly;
        private final AtomicInteger synCmdCount;
        private final Map<String, String> usrVariables = new LinkedHashMap<>();
        private final Map<String, String> sysVariables = new LinkedHashMap<>();

        /**
         * only for xa
         */
        StatusSync(int synCount) {
            this.schema = null;
            this.clientCharset = null;
            this.txIsolation = null;
            this.autocommit = null;
            this.readOnly = null;
            this.synCmdCount = new AtomicInteger(synCount);
        }

        StatusSync(String schema,
                   CharsetNames clientCharset, Integer txtIsolation, Boolean autocommit, Boolean readOnly,
                   int synCount, Map<String, String> usrVariables, Map<String, String> sysVariables, Set<String> toResetSys) {
            this.schema = schema;
            this.clientCharset = clientCharset;
            this.txIsolation = txtIsolation;
            this.autocommit = autocommit;
            this.readOnly = readOnly;
            this.synCmdCount = new AtomicInteger(synCount);
            this.usrVariables.putAll(usrVariables);
            this.sysVariables.putAll(sysVariables);
            for (String sysVariable : toResetSys) {
                this.sysVariables.remove(sysVariable);
            }
        }

        boolean synAndExecuted(BackendService service) {
            int remains = synCmdCount.decrementAndGet();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("synAndExecuted " + remains + ",conn info:" + service);
            }
            if (remains == 0) { // syn command finished
                this.updateConnectionInfo(service);
                service.metaDataSynced = true;
                return false;
            }
            return remains < 0;
        }

        private void updateConnectionInfo(BackendService service) {
            if (schema != null) {
                service.connection.setSchema(schema);
                service.connection.setOldSchema(schema);
            }
            if (clientCharset != null) {
                service.setCharsetName(clientCharset);
            }
            if (txIsolation != null) {
                service.txIsolation = txIsolation;
            }
            if (autocommit != null) {
                service.autocommit = autocommit;
            }
            if (readOnly != null) {
                service.sessionReadOnly = readOnly;
            }
            service.sysVariables.clear();
            service.usrVariables.clear();
            service.sysVariables.putAll(sysVariables);
            service.usrVariables.putAll(usrVariables);
        }
    }


    private class BackendOnetimeRunnable implements BackendRunnable {
        private final ThreadContext threadContext = new ThreadContext();

        @Override
        public ThreadContextView getThreadContext() {
            return threadContext;
        }

        @Override
        public void run() {
            try {
                handleTaskQueue(threadContext);
            } catch (Exception e) {
                handleDataError(e);
            } finally {
                isHandling.set(false);
                if (taskQueue.size() > 0) {
                    doHandle(null);
                }
            }
        }
    }
}
