/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble;

import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.xa.*;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.KVStoreRepository;
import com.actiontech.dble.buffer.DirectByteBufferPool;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.log.transaction.TxnLogProcessor;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketAcceptor;
import com.actiontech.dble.net.SocketConnector;
import com.actiontech.dble.net.executor.BackendCurrentRunnable;
import com.actiontech.dble.net.executor.FrontendBlockRunnable;
import com.actiontech.dble.net.executor.FrontendCurrentRunnable;
import com.actiontech.dble.net.executor.WriteToBackendRunnable;
import com.actiontech.dble.net.impl.aio.AIOAcceptor;
import com.actiontech.dble.net.impl.aio.AIOConnector;
import com.actiontech.dble.net.impl.nio.NIOAcceptor;
import com.actiontech.dble.net.impl.nio.NIOConnector;
import com.actiontech.dble.net.impl.nio.NIOReactorPool;
import com.actiontech.dble.net.mysql.WriteToBackendTask;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.services.factorys.ManagerConnectionFactory;
import com.actiontech.dble.services.factorys.ServerConnectionFactory;
import com.actiontech.dble.singleton.*;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.ExecutorUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class DbleServer {

    public static final String NAME = "Dble_";

    private static final DbleServer INSTANCE = new DbleServer();
    private static final Logger LOGGER = LoggerFactory.getLogger("Server");
    //used by manager command show @@binlog_status to get a stable GTID
    private final AtomicBoolean backupLocked = new AtomicBoolean(false);


    private volatile SystemVariables systemVariables = new SystemVariables();
    private TxnLogProcessor txnLogProcessor;

    private AsynchronousChannelGroup[] asyncChannelGroups;
    private AtomicInteger channelIndex = new AtomicInteger();

    private volatile int nextFrontProcessor;
    private volatile int nextBackendProcessor;

    private boolean aio = false;

    private final AtomicLong xaIDInc = new AtomicLong();

    public static DbleServer getInstance() {
        return INSTANCE;
    }

    private ServerConfig config;
    //dble server on/offline flag
    private final AtomicBoolean isOnline = new AtomicBoolean(true);
    private long startupTime;
    private IOProcessor[] frontProcessors;
    private IOProcessor[] backendProcessors;
    private SocketConnector connector;
    private ExecutorService businessExecutor;
    private ExecutorService backendBusinessExecutor;
    private ExecutorService writeToBackendExecutor;
    private ExecutorService complexQueryExecutor;
    private ExecutorService timerExecutor;
    private Map<String, ThreadWorkUsage> threadUsedMap = new ConcurrentHashMap<>();

    private Queue<ServiceTask> frontHandlerQueue;
    private BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;
    private Queue<ServiceTask> frontPriorityQueue;

    private Queue<ServiceTask> concurrentBackHandlerQueue;

    private volatile boolean startup = false;

    private DbleServer() {
    }


    public void startup() throws Exception {
        LOGGER.info("===========================================DBLE SERVER STARTING===================================");
        this.config = new ServerConfig();
        this.startupTime = TimeUtil.currentTimeMillis();
        LOGGER.info("=========================================Config file read finish==================================");

        LOGGER.info("=========================================Init Outer Ha Config==================================");
        HaConfigManager.getInstance().init();

        if (SystemConfig.getInstance().getEnableAlert() == 1) {
            AlertUtil.switchAlert(true);
        }
        AlertManager.getInstance().startAlert();
        LOGGER.info("========================================Alert Manager start finish================================");

        // server startup
        LOGGER.info("============================================Server start params===================================");
        LOGGER.info(NAME + "Server is ready to startup ...");

        LOGGER.info("system config params:" + SystemConfig.getInstance().toString());

        LOGGER.info("===========================================Init bufferPool start==================================");
        String inf = "Buffer pool info:[The count of pages is:" +
                SystemConfig.getInstance().getBufferPoolPageNumber() + ",every page size:" +
                SystemConfig.getInstance().getBufferPoolPageSize() +
                ", every page's chunk number(PageSize/ChunkSize) is:" +
                (SystemConfig.getInstance().getBufferPoolPageSize() / SystemConfig.getInstance().getBufferPoolChunkSize()) +
                "]";
        LOGGER.info(inf);
        BufferPoolManager.getInstance().init();
        LOGGER.info("===========================================Init bufferPool finish=================================");

        // startup processors
        int frontProcessorCount = SystemConfig.getInstance().getProcessors();
        int backendProcessorCount = SystemConfig.getInstance().getBackendProcessors();
        frontProcessors = new IOProcessor[frontProcessorCount];
        backendProcessors = new IOProcessor[backendProcessorCount];


        businessExecutor = ExecutorUtil.createFixed("BusinessExecutor", SystemConfig.getInstance().getProcessorExecutor());
        backendBusinessExecutor = ExecutorUtil.createFixed("backendBusinessExecutor", SystemConfig.getInstance().getBackendProcessorExecutor());
        writeToBackendExecutor = ExecutorUtil.createFixed("writeToBackendExecutor", SystemConfig.getInstance().getWriteToBackendExecutor());
        complexQueryExecutor = ExecutorUtil.createCached("complexQueryExecutor", SystemConfig.getInstance().getComplexExecutor());
        timerExecutor = ExecutorUtil.createFixed("Timer", 1);

        LOGGER.info("====================================Task Queue&Thread init start==================================");
        initTaskQueue();
        LOGGER.info("==================================Task Queue&Thread init finish===================================");


        for (int i = 0; i < frontProcessorCount; i++) {
            frontProcessors[i] = new IOProcessor("frontProcessor" + i, BufferPoolManager.getBufferPool());
        }
        for (int i = 0; i < backendProcessorCount; i++) {
            backendProcessors[i] = new IOProcessor("backendProcessor" + i, BufferPoolManager.getBufferPool());
        }

        if (SystemConfig.getInstance().getEnableSlowLog() == 1) {
            SlowQueryLog.getInstance().setEnableSlowLog(true);
        }

        LOGGER.info("==============================Connection  Connector&Acceptor init start===========================");
        // startup manager
        SocketAcceptor manager = null;
        SocketAcceptor server = null;

        aio = (SystemConfig.getInstance().getUsingAIO() == 1);
        if (aio) {
            int processorCount = frontProcessorCount + backendProcessorCount;
            LOGGER.info("using aio network handler ");
            asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
            initAioProcessor(processorCount);

            connector = new AIOConnector();
            manager = new AIOAcceptor(NAME + "Manager", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getManagerPort(), 100, new ManagerConnectionFactory(), this.asyncChannelGroups[0]);
            server = new AIOAcceptor(NAME + "Server", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getServerPort(), SystemConfig.getInstance().getServerBacklog(), new ServerConnectionFactory(), this.asyncChannelGroups[0]);
        } else {
            NIOReactorPool frontReactorPool = new NIOReactorPool(
                    DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIO_REACTOR_FRONT",
                    frontProcessorCount);
            NIOReactorPool backendReactorPool = new NIOReactorPool(
                    DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIO_REACTOR_BACKEND",
                    backendProcessorCount);

            connector = new NIOConnector(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", backendReactorPool);
            connector.start();

            manager = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Manager", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getManagerPort(), 100, new ManagerConnectionFactory(), frontReactorPool);
            server = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Server", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getServerPort(), SystemConfig.getInstance().getServerBacklog(), new ServerConnectionFactory(), frontReactorPool);
        }
        LOGGER.info("==========================Connection Connector&Acceptor init finish===============================");

        this.config.testConnection();
        LOGGER.info("==========================================Test connection finish==================================");

        // sync global status
        this.config.getAndSyncKeyVariables();
        LOGGER.info("=====================================Get And Sync KeyVariables finish=============================");

        // start transaction SQL log
        if (SystemConfig.getInstance().getRecordTxn() == 1) {
            txnLogProcessor = new TxnLogProcessor();
            txnLogProcessor.setName("TxnLogProcessor");
            txnLogProcessor.start();
        }

        SequenceManager.init(ClusterConfig.getInstance().getSequenceHandlerType());
        LOGGER.info("===================================Sequence manager init finish===================================");


        WriteQueueFlowController.init();
        LOGGER.info("===================================flow controller finish===================================");

        LOGGER.info("==============================Pull metaData from MySQL start======================================");
        pullVarAndMeta();
        LOGGER.info("==============================Pull metaData from MySQL finish=====================================");


        LOGGER.info("=========================================Init online status in cluster==================================");
        initOnlineStatus();

        FrontendUserManager.getInstance().initForLatest(config.getUsers(), SystemConfig.getInstance().getMaxCon());

        CacheService.getInstance().init(this.systemVariables.isLowerCaseTableNames());
        LOGGER.info("====================================Cache service init finish=====================================");

        LOGGER.info("=====================================Perform XA recovery log======================================");
        performXARecoveryLog();
        LOGGER.info("====================================Perform XA recovery finish====================================");

        manager.start();
        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());
        server.start();
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());
        LOGGER.info("=====================================Server started success=======================================");

        Scheduler.getInstance().init(timerExecutor);
        LOGGER.info("=======================================Scheduler started==========================================");

        CronScheduler.getInstance().init(config.getSchemas());
        LOGGER.info("====================================CronScheduler started=========================================");

        CustomMySQLHa.getInstance().start();
        LOGGER.info("======================================ALL START INIT FINISH=======================================");
        startup = true;
    }

    private void initAioProcessor(int processorCount) throws IOException {
        for (int i = 0; i < processorCount; i++) {
            asyncChannelGroups[i] = AsynchronousChannelGroup.withFixedThreadPool(processorCount,
                    new ThreadFactory() {
                        private int inx = 1;

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread th = new Thread(r);
                            th.setName(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "AIO" + (inx++));
                            LOGGER.info("created new AIO thread " + th.getName());
                            return th;
                        }
                    }
            );
        }
    }

    private void initTaskQueue() {
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {
            frontPriorityQueue = new ConcurrentLinkedQueue<>();
            frontHandlerQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < SystemConfig.getInstance().getProcessorExecutor(); i++) {
                businessExecutor.execute(new FrontendCurrentRunnable(frontHandlerQueue, frontPriorityQueue));
            }

            concurrentBackHandlerQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < SystemConfig.getInstance().getBackendProcessorExecutor(); i++) {
                backendBusinessExecutor.execute(new BackendCurrentRunnable(concurrentBackHandlerQueue));
            }

        } else {
            frontPriorityQueue = new ConcurrentLinkedQueue<>();
            frontHandlerQueue = new LinkedBlockingQueue<>();
            for (int i = 0; i < SystemConfig.getInstance().getProcessorExecutor(); i++) {
                businessExecutor.execute(new FrontendBlockRunnable(frontHandlerQueue, frontPriorityQueue));
            }

        }

        writeToBackendQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < SystemConfig.getInstance().getWriteToBackendExecutor(); i++) {
            writeToBackendExecutor.execute(new WriteToBackendRunnable(writeToBackendQueue));
        }
    }

    private void initDbGroup() {
        Map<String, PhysicalDbGroup> dbGroups = this.getConfig().getDbGroups();
        LOGGER.info("Initialize dbGroup ...");
        for (PhysicalDbGroup node : dbGroups.values()) {
            node.init("dble starts up");
        }
    }

    public void reloadSystemVariables(SystemVariables sys) {
        systemVariables = sys;
    }

    public IOProcessor nextFrontProcessor() {
        int i = ++nextFrontProcessor;
        if (i >= frontProcessors.length) {
            i = nextFrontProcessor = 0;
        }
        return frontProcessors[i];
    }

    public IOProcessor nextBackendProcessor() {
        int i = ++nextBackendProcessor;
        if (i >= backendProcessors.length) {
            i = nextBackendProcessor = 0;
        }
        return backendProcessors[i];
    }

    public BlockingQueue<List<WriteToBackendTask>> getWriteToBackendQueue() {
        return writeToBackendQueue;
    }

    public Map<String, ThreadWorkUsage> getThreadUsedMap() {
        return threadUsedMap;
    }


    public Queue<ServiceTask> getFrontHandlerQueue() {
        return frontHandlerQueue;
    }


    // check the closed/overtime connection
    public Runnable processorCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (IOProcessor p : backendProcessors) {
                                p.checkBackendCons();
                            }
                        } catch (Exception e) {
                            LOGGER.info("checkBackendCons caught err:" + e);
                        }
                    }
                });
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (IOProcessor p : frontProcessors) {
                                p.checkFrontCons();
                            }
                        } catch (Exception e) {
                            LOGGER.info("checkFrontCons caught err:", e);
                        }
                    }
                });
            }
        };
    }


    private void reviseSchemas() {
        if (systemVariables.isLowerCaseTableNames()) {
            config.reviseLowerCase();
            ConfigUtil.setSchemasForPool(config.getDbGroups(), config.getShardingNodes());
        } else {
            config.loadSequence();
            config.selfChecking0();
        }
    }

    private void initOnlineStatus() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            OnlineStatus.getInstance().mainThreadInitClusterOnline();
        }
    }

    private void pullVarAndMeta() throws IOException {
        ProxyMetaManager tmManager = new ProxyMetaManager();
        ProxyMeta.getInstance().setTmManager(tmManager);
        if (this.getConfig().isFullyConfigured()) {
            LOGGER.info("get variables Data start");
            //init for sys VAR
            VarsExtractorHandler handler = new VarsExtractorHandler(config.getDbGroups());
            SystemVariables newSystemVariables = handler.execute();
            if (newSystemVariables == null) {
                throw new IOException("Can't get variables from shardingNode");
            } else {
                systemVariables = newSystemVariables;
            }
            reviseSchemas();
            initDbGroup();
            LOGGER.info("get variables Data end");
            //init tmManager
            try {
                tmManager.init(this.getConfig());
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            reviseSchemas();
        }
    }


    // XA recovery log check
    private void performXARecoveryLog() {
        // fetch the recovery log
        CoordinatorLogEntry[] coordinatorLogEntries = getCoordinatorLogEntries();
        // init into in memory cached
        for (CoordinatorLogEntry coordinatorLogEntry1 : coordinatorLogEntries) {
            genXidSeq(coordinatorLogEntry1.getId());
            XAStateLog.flushMemoryRepository(coordinatorLogEntry1.getId(), coordinatorLogEntry1);
        }
        for (CoordinatorLogEntry coordinatorLogEntry : coordinatorLogEntries) {
            boolean needRollback = false;
            boolean needCommit = false;
            if (coordinatorLogEntry.getTxState() == TxState.TX_COMMIT_FAILED_STATE ||
                    // will committing, may send but failed receiving, should commit agagin
                    coordinatorLogEntry.getTxState() == TxState.TX_COMMITTING_STATE) {
                needCommit = true;
            } else if (coordinatorLogEntry.getTxState() == TxState.TX_ROLLBACK_FAILED_STATE ||
                    //don't konw prepare is succeed or not ,should rollback
                    coordinatorLogEntry.getTxState() == TxState.TX_PREPARE_UNCONNECT_STATE ||
                    // will rollbacking, may send but failed receiving,should rollback again
                    coordinatorLogEntry.getTxState() == TxState.TX_ROLLBACKING_STATE ||
                    // will preparing, may send but failed receiving,should rollback again
                    coordinatorLogEntry.getTxState() == TxState.TX_PREPARING_STATE) {
                needRollback = true;

            }
            if (needCommit || needRollback) {
                tryRecovery(coordinatorLogEntry, needCommit);
            }
        }
    }

    private void tryRecovery(CoordinatorLogEntry coordinatorLogEntry, boolean needCommit) {
        StringBuilder xaCmd = new StringBuilder();
        if (needCommit) {
            xaCmd.append("XA COMMIT ");
        } else {
            xaCmd.append("XA ROLLBACK ");
        }
        for (int j = 0; j < coordinatorLogEntry.getParticipants().length; j++) {
            ParticipantLogEntry participantLogEntry = coordinatorLogEntry.getParticipants()[j];
            // XA commit
            if (participantLogEntry.getTxState() != TxState.TX_COMMIT_FAILED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_COMMITTING_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARE_UNCONNECT_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ROLLBACKING_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ROLLBACK_FAILED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ENDED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARING_STATE) {
                continue;
            }
            outLoop:
            for (SchemaConfig schema : DbleServer.getInstance().getConfig().getSchemas().values()) {
                for (BaseTableConfig table : schema.getTables().values()) {
                    for (String shardingNode : table.getShardingNodes()) {
                        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
                        if (participantLogEntry.compareAddress(dn.getDbGroup().getWriteDbInstance().getConfig().getIp(), dn.getDbGroup().getWriteDbInstance().getConfig().getPort(), dn.getDatabase())) {
                            xaCmd.append(coordinatorLogEntry.getId(), 0, coordinatorLogEntry.getId().length() - 1);
                            xaCmd.append(".");
                            xaCmd.append(dn.getDatabase());
                            if (participantLogEntry.getExpires() != 0) {
                                xaCmd.append(".");
                                xaCmd.append(participantLogEntry.getExpires());
                            }
                            xaCmd.append("'");
                            XARecoverHandler handler = new XARecoverHandler(needCommit, participantLogEntry);
                            handler.execute(xaCmd.toString(), dn.getDatabase(), dn.getDbGroup().getWriteDbInstance());
                            if (!handler.isSuccess()) {
                                throw new RuntimeException("Fail to recover xa when dble start, please check backend mysql.");
                            }

                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(String.format("[%s] Host:[%s] sharding:[%s]", xaCmd, dn.getName(), dn.getDatabase()));
                            }

                            //reset xaCmd
                            xaCmd.setLength(0);
                            if (needCommit) {
                                xaCmd.append("XA COMMIT ");
                            } else {
                                xaCmd.append("XA ROLLBACK ");
                            }
                            break outLoop;
                        }
                    }
                }
            }
        }
        XAStateLog.saveXARecoveryLog(coordinatorLogEntry.getId(), needCommit ? TxState.TX_COMMITTED_STATE : TxState.TX_ROLLBACKED_STATE);
        XAStateLog.writeCheckpoint(coordinatorLogEntry.getId());
    }

    /**
     * covert the collection to array
     **/
    private CoordinatorLogEntry[] getCoordinatorLogEntries() {
        Repository fileRepository = ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode() ? new KVStoreRepository() : new FileSystemRepository();
        Collection<CoordinatorLogEntry> allCoordinatorLogEntries = fileRepository.getAllCoordinatorLogEntries(true);
        fileRepository.close();
        if (allCoordinatorLogEntries == null) {
            return new CoordinatorLogEntry[0];
        }
        if (allCoordinatorLogEntries.size() == 0) {
            return new CoordinatorLogEntry[0];
        }
        return allCoordinatorLogEntries.toArray(new CoordinatorLogEntry[allCoordinatorLogEntries.size()]);
    }

    public String genXaTxId() {
        long seq = this.xaIDInc.incrementAndGet();
        if (seq < 0) {
            synchronized (xaIDInc) {
                if (xaIDInc.get() < 0) {
                    xaIDInc.set(0);
                }
                seq = xaIDInc.incrementAndGet();
            }
        }
        StringBuilder id = new StringBuilder();
        id.append("'" + NAME + "Server.");
        id.append(SystemConfig.getInstance().getInstanceName());
        id.append(".");
        id.append(seq);
        id.append("'");
        return id.toString();
    }

    private void genXidSeq(String xaID) {
        String[] idSplit = xaID.replace("'", "").split("\\.");
        long seq = Long.parseLong(idSplit[2]);
        if (xaIDInc.get() < seq) {
            xaIDInc.set(seq);
        }
    }


    /**
     * get next AsynchronousChannel ,first is exclude if multi
     * AsynchronousChannelGroups
     *
     * @return AsynchronousChannelGroup
     */
    public AsynchronousChannelGroup getNextAsyncChannelGroup() {
        if (asyncChannelGroups.length == 1) {
            return asyncChannelGroups[0];
        } else {
            int index = (channelIndex.incrementAndGet()) % asyncChannelGroups.length;
            if (index == 0) {
                channelIndex.incrementAndGet();
                return asyncChannelGroups[1];
            } else {
                return asyncChannelGroups[index];
            }

        }
    }


    public boolean isAIO() {
        return aio;
    }


    public ExecutorService getTimerExecutor() {
        return timerExecutor;
    }

    public Queue<ServiceTask> getFrontPriorityQueue() {
        return frontPriorityQueue;
    }

    public ExecutorService getComplexQueryExecutor() {
        return complexQueryExecutor;
    }

    public AtomicBoolean getBackupLocked() {
        return backupLocked;
    }

    public boolean isBackupLocked() {
        return backupLocked.get();
    }


    public ServerConfig getConfig() {
        return config;
    }

    public IOProcessor[] getFrontProcessors() {
        return frontProcessors;
    }

    public IOProcessor[] getBackendProcessors() {
        return backendProcessors;
    }

    public SocketConnector getConnector() {
        return connector;
    }


    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    public SystemVariables getSystemVariables() {
        return systemVariables;
    }

    public TxnLogProcessor getTxnLogProcessor() {
        return txnLogProcessor;
    }

    public ExecutorService getBusinessExecutor() {
        return businessExecutor;
    }

    public ExecutorService getWriteToBackendExecutor() {
        return writeToBackendExecutor;
    }

    public ExecutorService getBackendBusinessExecutor() {
        return backendBusinessExecutor;
    }

    public boolean isStartup() {
        return startup;
    }


    public Queue<ServiceTask> getConcurrentBackHandlerQueue() {
        return concurrentBackHandlerQueue;
    }

    public void setConcurrentBackHandlerQueue(Queue<ServiceTask> concurrentBackHandlerQueue) {
        this.concurrentBackHandlerQueue = concurrentBackHandlerQueue;
    }

}
